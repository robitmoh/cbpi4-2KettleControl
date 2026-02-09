import asyncio
import logging
from cbpi.api import *
import time


@parameters([
    Property.Kettle(label="HLT_Kettle",
                    description="Select the HLT (Hot Liquor Tank) kettle. Its sensor and heater will be used for HLT temperature control."),
    Property.Actor(label="Mash_Pump",
                   description="CIRCULATION pump - circulates wort INSIDE the MASH tun. Runs when MASH is at target temperature (no heating needed). The HEATING pump is the MASH kettle's own Actor (heater)."),
    Property.Number(label="DeltaTemp", configurable=True, default_value=10,
                    description="HLT target = MASH target + DeltaTemp. Example: MASH target 65°C + delta 10 = HLT target 75°C"),
    Property.Number(label="Max_HLT_Temp", configurable=True, default_value=85,
                    description="Maximum HLT temperature limit. HLT target will never exceed this value regardless of DeltaTemp."),
    Property.Number(label="HLT_Hysteresis", configurable=True, default_value=0.5,
                    description="HLT heater hysteresis band in degrees. Heater ON when HLT < target - hysteresis, OFF when HLT >= target."),
    Property.Number(label="Mash_Hysteresis", configurable=True, default_value=0.3,
                    description="MASH pump switching hysteresis in degrees. Heating pump activates when MASH < target - hysteresis, circulation pump (agitator) activates when MASH >= target."),
    Property.Number(label="P", configurable=True, default_value=5.0,
                    description="MASH PID P value. Recommended: 2-10. Higher = starts heating sooner when temp drops below target."),
    Property.Number(label="I", configurable=True, default_value=0.01,
                    description="MASH PID I value. Recommended: 0.001-0.05. Keep low. Compensates for steady-state error over time."),
    Property.Number(label="D", configurable=True, default_value=5.0,
                    description="MASH PID D value. Recommended: 3-15. KEY for overshoot prevention: detects rate of temperature rise and stops heating BEFORE target. Increase if MASH overshoots, decrease if pumps switch too early."),
    Property.Select(label="SampleTime", options=[2, 5],
                    description="PID calculation interval in seconds. Default: 5"),
    Property.Number(label="Rest_Interval", configurable=True, default_value=600,
                    description="Agitator (circulation pump) work time in seconds before resting. Only used when MASH is at target. Default: 600 (10 min)"),
    Property.Number(label="Rest_Time", configurable=True, default_value=60,
                    description="Agitator (circulation pump) rest duration in seconds. Default: 60 (1 min)")])

class TwoKettleControl(CBPiKettleLogic):

    def __init__(self, cbpi, id, props):
        super().__init__(cbpi, id, props)
        self._logger = logging.getLogger(type(self).__name__)
        # MASH kettle references
        self.mash_kettle = None
        # HLT kettle references
        self.hlt_kettle = None
        self.hlt_heater = None
        # Pump references
        self.heater_pump = None  # mash_kettle.heater (actor) - circulates through HLT heat exchanger
        self.mash_pump = None    # Property.Actor - circulates inside MASH tun
        # PID and control variables
        self.pid = None
        self.sample_time = None
        self.needs_heating = False
        # HLT control parameters
        self.delta_temp = None
        self.max_hlt_temp = None
        self.hlt_hysteresis = None
        self.hlt_heater_on = False
        # MASH switching parameters
        self.mash_hysteresis = None
        # Pump rest parameters
        self.work_time = None
        self.rest_time = None

    async def on_stop(self):
        """Turns off all actors on stop."""
        await self.actor_off(self.hlt_heater)
        await self.actor_off(self.heater_pump)
        await self.actor_off(self.mash_pump)

    # ──────────────────────────────────────────────
    # HLT temperature control - hysteresis
    # ──────────────────────────────────────────────
    async def hlt_heater_control(self):
        """HLT heater control with hysteresis.
        HLT target = min(MASH_target + delta, Max_HLT_Temp)
        Turns on when HLT < target - hysteresis
        Turns off when HLT >= target
        """
        while self.running:
            try:
                hlt_temp = self.get_sensor_value(self.hlt_kettle.sensor).get("value")
                mash_target = self.get_kettle_target_temp(self.id)

                # Calculate HLT target temperature
                hlt_target = min(mash_target + self.delta_temp, self.max_hlt_temp)

                # Hysteresis control
                if hlt_temp < hlt_target - self.hlt_hysteresis:
                    if not self.hlt_heater_on:
                        self._logger.debug(
                            "HLT heater ON: HLT={:.1f}°C < target={:.1f}°C - hyst={:.1f}°C".format(
                                hlt_temp, hlt_target, self.hlt_hysteresis))
                        await self.actor_on(self.hlt_heater)
                        self.hlt_heater_on = True
                elif hlt_temp >= hlt_target:
                    if self.hlt_heater_on:
                        self._logger.debug(
                            "HLT heater OFF: HLT={:.1f}°C >= target={:.1f}°C".format(
                                hlt_temp, hlt_target))
                        await self.actor_off(self.hlt_heater)
                        self.hlt_heater_on = False
                # Within hysteresis band: maintain previous state

            except Exception as e:
                self._logger.error("HLT control error: {}".format(e))

            await asyncio.sleep(1)

    # ──────────────────────────────────────────────
    # MASH temperature control - PID with hysteresis
    # ──────────────────────────────────────────────
    async def mash_temp_control(self):
        """MASH PID control with hysteresis for pump switching.
        The PID D term detects the rate of temperature rise and stops heating
        BEFORE the target is reached, preventing overshoot in slow systems.
        - Stops heating when PID output <= 0 (D term anticipates overshoot)
          or as safety fallback when MASH temp >= target
        - Starts heating when MASH temp < target - mash_hysteresis AND PID output > 0
        """
        while self.running:
            try:
                mash_temp = self.get_sensor_value(self.mash_kettle.sensor).get("value")
                mash_target = self.get_kettle_target_temp(self.id)

                pid_output = self.pid.calc(mash_temp, mash_target)

                if self.needs_heating:
                    # Currently heating: PID decides when to stop
                    # D term sees temp rising and reduces output to 0 BEFORE target
                    # Safety: also stop if temp somehow reaches/exceeds target
                    if pid_output <= 0:
                        self.needs_heating = False
                        self.pid.reset_integral()
                        self._logger.info(
                            "PID stop heating: temp={:.1f}°C target={:.1f}°C PID={:.1f} -> CIRCULATION".format(
                                mash_temp, mash_target, pid_output))
                    elif mash_temp >= mash_target:
                        self.needs_heating = False
                        self.pid.reset_integral()
                        self._logger.info(
                            "Safety stop: temp={:.1f}°C >= target={:.1f}°C -> CIRCULATION".format(
                                mash_temp, mash_target))
                else:
                    # Currently circulating: start heating when temp drops below threshold
                    if mash_temp < mash_target - self.mash_hysteresis and pid_output > 0:
                        self.needs_heating = True
                        self._logger.info(
                            "MASH needs heating: {:.1f}°C < {:.1f}°C (hyst={:.1f}) PID={:.1f} -> HEATING".format(
                                mash_temp, mash_target - self.mash_hysteresis,
                                self.mash_hysteresis, pid_output))

                self._logger.debug(
                    "MASH PID: temp={:.1f} target={:.1f} output={:.1f} heating={}".format(
                        mash_temp, mash_target, pid_output, self.needs_heating))

            except Exception as e:
                self._logger.error("MASH control error: {}".format(e))

            await asyncio.sleep(self.sample_time)

    # ──────────────────────────────────────────────
    # Pump control - mutual exclusion
    # ──────────────────────────────────────────────
    async def pump_control(self):
        """Pump switching based on the needs_heating flag.
        When heating needed: heater_pump ON, mash_pump OFF
        When no heating needed: heater_pump OFF, mash_pump ON (with rest interval)
        The two pumps never run simultaneously.
        """
        while self.running:
            if self.needs_heating:
                # Heating mode: heater pump circulates through HLT heat exchanger
                await self.actor_off(self.mash_pump)
                await self.actor_on(self.heater_pump)
                self._logger.info("Heating mode: heater pump (actor) ON, agitator OFF")
                # Wait while heating is needed, check every 1 second
                while self.running and self.needs_heating:
                    await asyncio.sleep(1)
                # Heating done, turn off heater pump
                await self.actor_off(self.heater_pump)
            else:
                # Circulation mode: agitator circulates with rest interval
                await self.actor_off(self.heater_pump)
                await self.actor_on(self.mash_pump)
                self._logger.info("Circulation mode: agitator ON ({:.0f}s), heater pump (actor) OFF".format(self.work_time))

                # Mash pump runs for work_time (or until heating is needed)
                off_time = time.time() + self.work_time
                while self.running and not self.needs_heating and time.time() < off_time:
                    await asyncio.sleep(1)

                # If heating is needed, immediately return to loop start
                if self.needs_heating:
                    continue

                # Rest: mash pump off for rest_time
                await self.actor_off(self.mash_pump)
                self._logger.info("Agitator resting ({:.0f}s)".format(self.rest_time))
                rest_end = time.time() + self.rest_time
                while self.running and not self.needs_heating and time.time() < rest_end:
                    await asyncio.sleep(1)

    # ──────────────────────────────────────────────
    # Main run loop
    # ──────────────────────────────────────────────
    async def run(self):
        self._logger = logging.getLogger(type(self).__name__)
        try:
            # PID parameters
            self.sample_time = int(self.props.get("SampleTime", 5))
            p = float(self.props.get("P", 5.0))
            i = float(self.props.get("I", 0.01))
            d = float(self.props.get("D", 5.0))
            self.pid = PIDArduino(self.sample_time, p, i, d, 0, 100)

            # HLT parameters
            self.delta_temp = float(self.props.get("DeltaTemp", 10))
            self.max_hlt_temp = float(self.props.get("Max_HLT_Temp", 85))
            self.hlt_hysteresis = float(self.props.get("HLT_Hysteresis", 0.5))

            # MASH switching parameters
            self.mash_hysteresis = float(self.props.get("Mash_Hysteresis", 0.3))

            # Pump rest parameters
            self.work_time = float(self.props.get("Rest_Interval", 600))
            self.rest_time = float(self.props.get("Rest_Time", 60))

            # MASH kettle (the one this logic is assigned to)
            self.mash_kettle = self.get_kettle(self.id)
            # Heater pump = MASH kettle's actor (heater) - circulates through HLT heat exchanger
            self.heater_pump = self.mash_kettle.heater
            # Circulation pump = from Property.Actor - circulates inside MASH tun
            self.mash_pump = self.props.get("Mash_Pump", None)
            if self.mash_pump is None:
                raise ValueError("Mash_Pump is not configured!")

            # HLT kettle (from Property.Kettle)
            hlt_kettle_id = self.props.get("HLT_Kettle", None)
            if hlt_kettle_id is None:
                raise ValueError("HLT_Kettle is not configured!")
            self.hlt_kettle = self.get_kettle(hlt_kettle_id)
            self.hlt_heater = self.hlt_kettle.heater

            self.needs_heating = False
            self.hlt_heater_on = False

            logging.info("2KettleControl started | PID P:{} I:{} D:{} | HLT delta:{} max:{} hyst:{} | "
                         "MASH sensor:{} heater_pump:{} mash_pump:{} | HLT sensor:{} hlt_heater:{}".format(
                p, i, d, self.delta_temp, self.max_hlt_temp, self.hlt_hysteresis,
                self.mash_kettle.sensor, self.heater_pump, self.mash_pump,
                self.hlt_kettle.sensor, self.hlt_heater))

            # Start 3 parallel control tasks
            hlt_task = asyncio.create_task(self.hlt_heater_control())
            mash_task = asyncio.create_task(self.mash_temp_control())
            pump_task = asyncio.create_task(self.pump_control())

            await asyncio.gather(hlt_task, mash_task, pump_task)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error("2KettleControl error: {}".format(e))
        finally:
            self.running = False
            await self.actor_off(self.hlt_heater)
            await self.actor_off(self.heater_pump)
            await self.actor_off(self.mash_pump)

# Based on Arduino PID Library
# See https://github.com/br3ttb/Arduino-PID-Library
class PIDArduino(object):

    def __init__(self, sampleTimeSec, kp, ki, kd, outputMin=float('-inf'),
                 outputMax=float('inf'), getTimeMs=None):
        if kp is None:
            raise ValueError('kp must be specified')
        if ki is None:
            raise ValueError('ki must be specified')
        if kd is None:
            raise ValueError('kd must be specified')
        if float(sampleTimeSec) <= float(0):
            raise ValueError('sampleTimeSec must be greater than 0')
        if outputMin >= outputMax:
            raise ValueError('outputMin must be less than outputMax')

        self._logger = logging.getLogger(type(self).__name__)
        self._Kp = kp
        self._Ki = ki * sampleTimeSec
        self._Kd = kd / sampleTimeSec
        self._sampleTime = sampleTimeSec * 1000
        self._outputMin = outputMin
        self._outputMax = outputMax
        self._iTerm = 0
        self._lastInput = 0
        self._lastOutput = 0
        self._lastCalc = 0

        if getTimeMs is None:
            self._getTimeMs = self._currentTimeMs
        else:
            self._getTimeMs = getTimeMs

    def reset_integral(self):
        """Reset the integral term to prevent windup when switching modes."""
        self._iTerm = 0

    def calc(self, inputValue, setpoint):
        now = self._getTimeMs()

        if (now - self._lastCalc) < self._sampleTime:
            return self._lastOutput

        # Compute all the working error variables
        error = setpoint - inputValue
        dInput = inputValue - self._lastInput

        # In order to prevent windup, only integrate if the process is not saturated
        if self._lastOutput < self._outputMax and self._lastOutput > self._outputMin:
            self._iTerm += self._Ki * error
            self._iTerm = min(self._iTerm, self._outputMax)
            self._iTerm = max(self._iTerm, self._outputMin)

        p = self._Kp * error
        i = self._iTerm
        d = -(self._Kd * dInput)

        # Compute PID Output
        self._lastOutput = p + i + d
        self._lastOutput = min(self._lastOutput, self._outputMax)
        self._lastOutput = max(self._lastOutput, self._outputMin)

        # Log some debug info
        self._logger.debug('P: {0}'.format(p))
        self._logger.debug('I: {0}'.format(i))
        self._logger.debug('D: {0}'.format(d))
        self._logger.debug('output: {0}'.format(self._lastOutput))

        # Remember some variables for next time
        self._lastInput = inputValue
        self._lastCalc = now
        return self._lastOutput

    def _currentTimeMs(self):
        return time.time() * 1000

def setup(cbpi):

    '''
    This method is called by the server during startup 
    Here you need to register your plugins at the server
    
    :param cbpi: the cbpi core 
    :return: 
    '''

    cbpi.plugin.register("2KettleControl", TwoKettleControl)

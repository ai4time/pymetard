import numpy as np


def saturation_vapor_pressure(temperature_celcius: float) -> float:
    return 611.2 * np.exp(
        17.67 * temperature_celcius / (temperature_celcius + 243.5)
    )


def relative_humidity_from_dewpoint(
    temperature_celcius: float,
    dewpoint_celcius: float,
) -> float:
    e = saturation_vapor_pressure(dewpoint_celcius)
    e_s = saturation_vapor_pressure(temperature_celcius)
    return (e / e_s)

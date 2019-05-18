import numpy as np
import matplotlib.pyplot as plt
N_SAMPLES = 500

input_range = np.linspace(0, 2*np.pi, N_SAMPLES, dtype=np.float)
signal = np.sin(input_range)
noise = np.random.normal(0, 1, N_SAMPLES)
assert noise.shape == input_range.shape

measurements = signal + noise/5


plt.plot(input_range, measurements, 'b.', alpha = 0.5)
plt.plot(input_range, signal, 'r-', linewidth = 4)
plt.legend(['Sensor readings', 'Truth'])
plt.xlabel('Time')
plt.ylabel('Value of thing we care about')
plt.show()


del signal
from statsmodels.nonparametric.smoothers_lowess import lowess

filtered = lowess(measurements, input_range, frac=0.1)
plt.plot(input_range, measurements, 'b.', alpha=0.5)
plt.plot(filtered[:, 0], filtered[:, 1], 'r-', linewidth=4)
plt.legend(['Senser readings', 'Reconstructed signal'])
plt.show()
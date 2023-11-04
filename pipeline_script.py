import matplotlib.pyplot as plt
import time
import random
import numpy as np

def testFunc():
  time.sleep(random.randint(0, 1))

def testFunc2():
  time.sleep(random.randint(0, 1) * 2)


FILEPATHS = ["afads", "hbshdgh", "rkyumrc"]
CHUNKSIZES = ["2x2", "3x3", "4x4"]
TESTCASES = [testFunc, testFunc2]
  

def getTestTime(filepath) -> int:
  times = []
  for func in TESTCASES:
    startTime = time.time()

    func()

    endTime = time.time()
    times.append(endTime - startTime)

  return times

if __name__ == "__main__":
  times = []
  for filepath in FILEPATHS:
    times.append(getTestTime(filepath))

  times = np.array(times)

  for idx, func in enumerate(TESTCASES):
    # Create a bar chart
    plt.bar(CHUNKSIZES, times[:, idx])

    # Add labels and title
    plt.xlabel('Categories')
    plt.ylabel(f'Time for test case {idx}')

    # Save the chart to a file (e.g., as a PNG image)
    plt.savefig(f'bar_chart_{idx}.png')

  

import xarray as xr
import time
from tqdm import tqdm

small = (slice(0, 8), slice(0, 7))
medium = (slice(0, 80), slice(0, 70))
large = (slice(0, 820), slice(0, 1210))


# Test Cases
def Bench_MaxRain(dataset: xr.Dataset):
    values = dataset["RAINRATE"].max().compute().values


# Function
def DoWork(dataset, size, time):
    area = dataset.isel(x=size[0], y=size[1], time=time)
    values = area["RAINRATE"].mean(dim="time").compute().values
    return values


# Day ===============================================
def Average_Small_Day(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=small, time=slice(0, 24))


def Average_Med_Day(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=medium, time=slice(0, 24))


def Average_Large_Day(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=large, time=slice(0, 24))


# Week ===============================================
def Average_Small_Week(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=small, time=slice(0, 24 * 7))


def Average_Med_Week(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=medium, time=slice(0, 24 * 7))


def Average_Large_Week(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=large, time=slice(0, 24 * 7))


# Month ===============================================
def Average_Small_Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=small, time=slice(0, 24 * 7 * 4))


def Average_Med_Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=medium, time=slice(0, 24 * 7 * 4))


def Average_Large_Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=large, time=slice(0, 24 * 7 * 4))


# 6 Month ===============================================
def Average_Small_6Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=small, time=slice(0, 24 * 7 * 4 * 6))


def Average_Med_6Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=medium, time=slice(0, 24 * 7 * 4 * 6))


def Average_Large_6Month(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=large, time=slice(0, 24 * 7 * 4 * 6))


# Year ===============================================
def Average_Small_Year(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=small, time=slice(0, 24 * 7 * 4))


def Average_Med_Year(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=medium, time=slice(0, 24 * 7 * 4 * 6))


def Average_Large_Year(dataset: xr.Dataset):
    DoWork(dataset=dataset, size=large, time=slice(0, 24 * 7 * 4 * 6))


# Used for the test cases
testcases = [
    {"name": "Max_Rain_Test", "func": Bench_MaxRain},
    {"name": "Average_Small_Day", "func": Average_Small_Day},
    {"name": "Average_Med_Day", "func": Average_Med_Day},
    {"name": "Average_Large_Day", "func": Average_Large_Day},
    {"name": "Average_Small_Week", "func": Average_Small_Week},
    {"name": "Average_Med_Week", "func": Average_Med_Week},
    {"name": "Average_Large_Week", "func": Average_Large_Week},
    {"name": "Average_Small_Month", "func": Average_Small_Month},
    {"name": "Average_Med_Month", "func": Average_Med_Month},
    {"name": "Average_Large_Month", "func": Average_Large_Month},
    {"name": "Average_Small_6_Month", "func": Average_Small_6Month},
    {"name": "Average_Med_6_Month", "func": Average_Med_6Month},
    {"name": "Average_Large_6_Month", "func": Average_Large_6Month},
    {"name": "Average_Small_Year", "func": Average_Small_Year},
    {"name": "Average_Med_Year", "func": Average_Med_Year},
    {"name": "Average_Large_Year", "func": Average_Large_Year},
]

NUM_REPETITIONS = 5


def GetTestTime(datatset, testcases) -> int:
    times = []

    for func in (bar := tqdm(testcases, leave=False, ascii=" ░▒█")):
        bar.set_description(f"    └───{func['name']}")

        timetoadd = 0
        startTime = time.time()

        for i in range(NUM_REPETITIONS):
            func["func"](datatset)

        endTime = time.time()
        timetoadd += endTime - startTime
        timetoadd = timetoadd / NUM_REPETITIONS

        times.append(timetoadd)

    return times

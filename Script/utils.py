class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def printHeader(string) -> None:
  print(bcolors.HEADER + string + bcolors.ENDC)

def printGreen(string) -> None:
  print(bcolors.OKGREEN + string + bcolors.ENDC)

def printRed(string) -> None:
  print(bcolors.FAIL + string + bcolors.ENDC)
import sys
# import threading
import utils
from scu import SCU

HANAKO = "169.254.229.153"
TARO = "169.254.155.219"

def main():
    # TODO
    scu = SCU(mtu = 1500)
    scu.bind_as_receiver(receiver_address = (HANAKO, 8899))
    for i in range(0, 1000):
        filedata = scu.recv()
        utils.write_file(f"./data/data{i}", filedata)
        print(f"file received: {i}", end="\r")

if __name__ == '__main__':
    main()

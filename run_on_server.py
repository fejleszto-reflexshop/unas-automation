import os
import time

def main() -> None:
    # TODO: wait for a signal to run commands

    while True:
        time.sleep(60)

        os.system("python unas_actions.py")
        os.system("python google_cloud_actions.py")


if __name__ == "__main__":
    main()

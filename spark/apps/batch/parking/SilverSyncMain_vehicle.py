from SilverMainTableProcessor import ChangeProcessor

if __name__ == "__main__":
    processor = ChangeProcessor("vehicle")
    processor.sync_to_main()

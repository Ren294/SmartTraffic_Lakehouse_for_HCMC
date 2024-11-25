from SilverMainTableProcessor import ChangeProcessor

if __name__ == "__main__":
    processor = ChangeProcessor("owner")
    processor.sync_to_main()

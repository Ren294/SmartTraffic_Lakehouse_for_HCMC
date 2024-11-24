from SilverMainTableProcessor import ChangeProcessor

if __name__ == "__main__":
    processor = ChangeProcessor("inventorytransaction")
    processor.sync_to_main()

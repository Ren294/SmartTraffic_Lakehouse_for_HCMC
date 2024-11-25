from SilverStagingTableProcessor import ChangeProcessor
if __name__ == "__main__":
    processor = ChangeProcessor("vehicle")
    processor.update_hudi()

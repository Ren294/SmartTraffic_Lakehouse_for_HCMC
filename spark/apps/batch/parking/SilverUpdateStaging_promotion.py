from SilverStagingTableProcessor import ChangeProcessor
if __name__ == "__main__":
    processor = ChangeProcessor("promotion")
    processor.update_hudi()

from SilverStagingTableProcessor import ChangeProcessor
if __name__ == "__main__":
    processor = ChangeProcessor("feedback")
    processor.update_hudi()

from SilverStagingTableProcessor import ChangeProcessor
if __name__ == "__main__":
    processor = ChangeProcessor("staff")
    processor.update_hudi()

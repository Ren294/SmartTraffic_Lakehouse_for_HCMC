from SilverStagingTableProcessor import ChangeProcessor
if __name__ == "__main__":
    processor = ChangeProcessor("payment")
    processor.update_hudi()

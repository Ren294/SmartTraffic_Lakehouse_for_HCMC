"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from SilverMainTableProcessor import ChangeProcessor

if __name__ == "__main__":
    processor = ChangeProcessor("gasstation")
    processor.check_changes()

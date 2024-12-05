"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from typing import Dict


class TableConfig:

    TABLES = {
        'owner': {
            'record_key': 'ownerid',
            'partition_fields': [],
            'compare_columns': ['name', 'contactinfo', 'email', 'phonenumber']
        },
        'feedback': {
            'record_key': 'feedbackid',
            'partition_fields': [],
            'compare_columns': ['ownerid', 'rating', 'comment', 'feedbackdate']
        },
        'parkinglot': {
            'record_key': 'parkinglotid',
            'partition_fields': [],
            'compare_columns': ['name', 'location', 'totalspaces', 'availablespaces',
                                'carspaces', 'motorbikespaces', 'bicyclespaces', 'type', 'hourlyrate']
        },
        'parkingrecord': {
            'record_key': 'recordid',
            'partition_fields': [],
            'compare_columns': ['parkinglotid', 'vehicleid', 'checkintime', 'checkouttime', 'fee']
        },
        'payment': {
            'record_key': 'paymentid',
            'partition_fields': [],
            'compare_columns': ['recordid', 'amountpaid', 'paymentmethod', 'paymentdate']
        },
        'promotion': {
            'record_key': 'promotionid',
            'partition_fields': [],
            'compare_columns': ['description', 'discountrate', 'startdate', 'enddate']
        },
        'staff': {
            'record_key': 'staffid',
            'partition_fields': [],
            'compare_columns': ['name', 'role', 'contactinfo', 'shiftstarttime', 'shiftendtime']
        },
        'vehicle': {
            'record_key': 'vehicleid',
            'partition_fields': [],
            'compare_columns': ['licenseplate', 'vehicletype', 'ownerid', 'color', 'brand']
        }
    }

    @classmethod
    def get_table_config(cls, table_name: str) -> Dict:
        return cls.TABLES.get(table_name)

    @classmethod
    def get_hudi_options(cls, table_name: str, operation: str = 'upsert') -> Dict[str, str]:
        table_config = cls.get_table_config(table_name)
        options = {
            'hoodie.table.name': f'parking_{table_name}',
            'hoodie.datasource.write.recordkey.field': table_config['record_key'],
            'hoodie.datasource.write.precombine.field': 'last_update',
            'hoodie.datasource.write.operation': operation,
            'hoodie.upsert.shuffle.parallelism': '2',
            'hoodie.insert.shuffle.parallelism': '2'
        }

        if table_config['partition_fields']:
            options['hoodie.datasource.write.partitionpath.field'] = ','.join(
                table_config['partition_fields'])

        return options

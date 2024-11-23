from typing import Dict


class TableConfig:
    """Configuration class for table definitions"""

    TABLES = {
        'customer': {
            'record_key': 'customerid',
            'partition_fields': [],
            'compare_columns': ['customername', 'address', 'phonenumber', 'email', 'notes', 'vehicletypename', 'licenseplate']
        },
        'employee': {
            'record_key': 'employeeid',
            'partition_fields': [],
            'compare_columns': ['employeename', 'position', 'gasstationid', 'phonenumber', 'email', 'startdate', 'address']
        },
        'gasstation': {
            'record_key': 'gasstationid',
            'partition_fields': [],
            'compare_columns': ['gasstationname', 'address', 'phonenumber', 'email', 'notes']
        },
        'inventorytransaction': {
            'record_key': 'transactionid',
            'partition_fields': [],
            'compare_columns': ['tankid', 'quantityin', 'quantityout', 'remainingquantity', 'transactiondate']
        },
        'invoice': {
            'record_key': 'invoiceid',
            'partition_fields': [],
            'compare_columns': ['customerid', 'employeeid', 'gasstationid', 'issuedate', 'totalamount', 'paymentmethod']
        },
        'invoicedetail': {
            'record_key': 'invoicedetailid',
            'partition_fields': [],
            'compare_columns': ['invoiceid', 'productid', 'quantitysold', 'sellingprice', 'totalprice']
        },
        'product': {
            'record_key': 'productid',
            'partition_fields': [],
            'compare_columns': ['productname', 'unitprice', 'producttype', 'supplier', 'stockquantity']
        },
        'storagetank': {
            'record_key': 'tankid',
            'partition_fields': [],
            'compare_columns': ['gasstationid', 'tankname', 'capacity', 'materialtype', 'currentquantity']
        }
    }

    @classmethod
    def get_table_config(cls, table_name: str) -> Dict:
        """Get configuration for specific table"""
        return cls.TABLES.get(table_name)

    @classmethod
    def get_hudi_options(cls, table_name: str, operation: str = 'upsert') -> Dict[str, str]:
        """Get Hudi options for table"""
        table_config = cls.get_table_config(table_name)
        options = {
            'hoodie.table.name': f'gasstation_{table_name}',
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

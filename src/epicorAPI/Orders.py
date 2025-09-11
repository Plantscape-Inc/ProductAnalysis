import pymssql
import polars as pl

from src.epicorAPI.utils import sqlexec, sqlexec_local


def trimmed_dtl(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "company",
        "ordernum",
        "orderline",
        "linetype",
        "partnum",
        "linedesc",
        "commissionable",
        "discountpercent",
        "unitprice",
        "docunitprice",
        "orderqty",
        "discount",
        "docdiscount",
        "requestdate",
        "prodcode",
        "xpartnum",
        "pricepercode",
        "ordercomment",
        "shipcomment",
        "invoicecomment",
        "picklistcomment",
        "quotenum",
        "quoteline",
        "needbydate",
        "custnum",
        "basepartnum",
        "warranty",
        "warrantycode",
        "sellingfactor",
        "sellingquantity",
        "mktgcampaignid",
        "mktgevntseq",
        "lockqty",
        "pricelistcode",
        "breaklistcode",
        "lockprice",
        "listprice",
        "doclistprice",
        "ordbasedprice",
        "docordbasedprice",
        "pricegroupcode",
        "overridepricelist",
        "baserevisionnum",
        "pricingvalue",
        "displayseq",
        "kitparentline",
        "sellingfactordirection",
        "repsplit1",
        "changedby",
        "changedate",
        "changetime",
        "reversecharge",
        "totalreleases",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def trimmed_rel(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "company",
        "ordernum",
        "orderline",
        "orderrelnum",
        "linetype",
        "reqdate",
        "ourreqqty",
        "shiptonum",
        "shipviacode",
        "openrelease",
        "firmrelease",
        "make",
        "ourjobqty",
        "voidrelease",
        "ourstockqty",
        "warehousecode",
        "partnum",
        "revisionnum",
        "shpconnum",
        "needbydate",
        "plant",
        "sellingreqqty",
        "sellingjobqty",
        "changedby",
        "changedate",
        "changetime",
        "taxconnectcalc",
        "getdflttaxids",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def trimmed_hed(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "openorder",
        "voidorder",
        "company",
        "ordernum",
        "custnum",
        "ponum",
        "orderheld",
        "entryperson",
        "shiptonum",
        "requestdate",
        "orderdate",
        "fob",
        "shipviacode",
        "termscode",
        "discountpercent",
        "prcconnum",
        "shpconnum",
        "salesreplist",
        "ordercomment",
        "shipcomment",
        "invoicecomment",
        "picklistcomment",
        "depositbal",
        "docdepositbal",
        "needbydate",
        "character03",
        "date03",
        "checkbox03",
        "exchangerate",
        "currencycode",
        "lockrate",
        "refcode",
        "reftobaserate",
        "expirationmonth",
        "expirationyear",
        "cardid",
        "autoorderbaseddisc",
        "entrymethod",
        "shortchar03",
        "lockqty",
        "ccamount",
        "ccfreight",
        "cctax",
        "cctotal",
        "ccdocamount",
        "ccdocfreight",
        "ccdoctax",
        "ccdoctotal",
        "btcustnum",
        "btconnum",
        "reprate4",
        "reprate5",
        "repsplit1",
        "intrntlship",
        "changedby",
        "changedate",
        "changetime",
        "readytocalc",
        "totalcharges",
        "totalmisc",
        "totaldiscount",
        "totalcomm",
        "totaladvbill",
        "totallines",
        "totalreleases",
        "totalreldates",
        "doctotalcharges",
        "doctotalmisc",
        "doctotaldiscount",
        "doctotalcomm",
        "totaltax",
        "doctotaltax",
        "doctotaladvbill",
        "totalshipped",
        "totalinvoiced",
        "totalcommlines",
        "srcommableamt1",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def clean_dtl(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "ordernum",
        "orderline",
        "partnum",
        "linedesc",
        "discountpercent",
        "unitprice",
        "docunitprice",
        "orderqty",
        "discount",
        "docdiscount",
        "requestdate",
        "prodcode",
        "xpartnum",
        "ordercomment",
        "shipcomment",
        "invoicecomment",
        "picklistcomment",
        "quotenum",
        "quoteline",
        "needbydate",
        "custnum",
        "basepartnum",
        "warranty",
        "sellingquantity",
        "listprice",
        "doclistprice",
        "ordbasedprice",
        "docordbasedprice",
        "baserevisionnum",
        "pricingvalue",
        "sellingfactordirection",
        "repsplit1",
        "changedby",
        "changedate",
        "changetime",
        "reversecharge",
        "totalreleases",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def clean_rel(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "company",
        "ordernum",
        "orderline",
        "orderrelnum",
        "linetype",
        "reqdate",
        "ourreqqty",
        "shiptonum",
        "shipviacode",
        "openrelease",
        "firmrelease",
        "make",
        "ourjobqty",
        "voidrelease",
        "ourstockqty",
        "warehousecode",
        "partnum",
        "revisionnum",
        "shpconnum",
        "needbydate",
        "plant",
        "sellingreqqty",
        "sellingjobqty",
        "changedby",
        "changedate",
        "changetime",
        "taxconnectcalc",
        "getdflttaxids",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def clean_hed(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "openorder",
        "voidorder",
        "company",
        "ordernum",
        "custnum",
        "ponum",
        "orderheld",
        "entryperson",
        "shiptonum",
        "requestdate",
        "orderdate",
        "fob",
        "shipviacode",
        "termscode",
        "discountpercent",
        "prcconnum",
        "shpconnum",
        "salesreplist",
        "ordercomment",
        "shipcomment",
        "invoicecomment",
        "picklistcomment",
        "depositbal",
        "docdepositbal",
        "needbydate",
        "character03",
        "date03",
        "checkbox03",
        "exchangerate",
        "currencycode",
        "lockrate",
        "refcode",
        "reftobaserate",
        "expirationmonth",
        "expirationyear",
        "cardid",
        "autoorderbaseddisc",
        "entrymethod",
        "shortchar03",
        "lockqty",
        "ccamount",
        "ccfreight",
        "cctax",
        "cctotal",
        "ccdocamount",
        "ccdocfreight",
        "ccdoctax",
        "ccdoctotal",
        "btcustnum",
        "btconnum",
        "reprate4",
        "reprate5",
        "repsplit1",
        "intrntlship",
        "changedby",
        "changedate",
        "changetime",
        "readytocalc",
        "totalcharges",
        "totalmisc",
        "totaldiscount",
        "totalcomm",
        "totaladvbill",
        "totallines",
        "totalreleases",
        "totalreldates",
        "doctotalcharges",
        "doctotalmisc",
        "doctotaldiscount",
        "doctotalcomm",
        "totaltax",
        "doctotaltax",
        "doctotaladvbill",
        "totalshipped",
        "totalinvoiced",
        "totalcommlines",
        "srcommableamt1",
        "PROGRESS_RECID",
        "PROGRESS_RECID_IDENT_"
    ]

    return df.select(columns)


def fetch_order(connection: pymssql._pymssql.Connection, order_id: str, clean=False):
    cursor = connection.cursor()

    order_dtl = sqlexec(cursor, f"""SELECT *
                   FROM orderdtl
                   where ordernum = {order_id};""")

    order_rel = sqlexec(cursor, f"""SELECT *
                   FROM orderrel
                   where ordernum = {order_id};""")

    order_hed = sqlexec(cursor, f"""SELECT *
                   FROM orderhed
                   where ordernum = {order_id};""")

    custnum = order_dtl['custnum'][0]

    order_ship = fetch_order(connection, custnum, clean=clean)

    if clean:
        order_dtl = clean_dtl(order_dtl)
        order_rel = clean_rel(order_rel)
        order_hed = clean_hed(order_hed)

    order_dtl = order_dtl.to_dicts()
    order_rel = order_rel.to_dicts()
    order_hed = order_hed.to_dicts()

    return {"orderhed": order_hed[0], "orderdtl": order_dtl, "orderrel": order_rel, "order_shipping": order_ship}


def fetch_order_local(order_id: str, clean=False):
    order_dtl = sqlexec_local("orderdtl", f"""SELECT *
                   FROM orderdtl
                   where ordernum = {order_id};""")

    order_rel = sqlexec_local("orderrel", f"""SELECT *
                   FROM orderrel
                   where ordernum = {order_id};""")

    order_hed = sqlexec_local("orderhed", f"""SELECT *
                   FROM orderhed
                   where ordernum = {order_id};""")

    custnum = order_hed['custnum'][0]

    customer = fetch_customer_local(custnum, clean=clean)

    if clean:
        order_dtl = clean_dtl(order_dtl)
        order_rel = clean_rel(order_rel)
        order_hed = clean_hed(order_hed)

    order_dtl = order_dtl.to_dicts()
    order_rel = order_rel.to_dicts()
    order_hed = order_hed.to_dicts()

    return {"orderhed": order_hed[0], "orderdtl": order_dtl, "orderrel": order_rel, "customerInfo": customer}


def fetch_customer(connection: pymssql._pymssql.Connection, custid: str, clean=False):
    cursor = connection.cursor()
    customer = sqlexec(cursor, f"""
        SELECT * FROM customer WHERE custnum = '{custid.strip()}';
    """)

    return customer.to_dicts()[0] if len(customer) > 0 else None


def fetch_customer_local(custid: str, clean=False):
    customer = sqlexec_local("customer",
                             f"""
                SELECT * from customer WHERE custnum = {custid};"""
                             )

    return customer.to_dicts()[0]

# 20927

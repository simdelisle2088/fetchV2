QUERY_GET_UPC = """
SELECT
    IN_UPC.SKU, IN_UPC.Upc as upc
FROM
    m.IN_UPC as IN_UPC
Where IN_UPC.SKU = '{0}'
"""

QUERY_GET_CLIENTS = """
SELECT
    POS_ARC_D_HEAD.CrName, POS_ARC_D_HEAD.CrName as customer_name,
    POS_ARC_D_HEAD.CrAddr1, POS_ARC_D_HEAD.CrAddr1 as Address1,
    POS_ARC_D_HEAD.CrAddr2, POS_ARC_D_HEAD.CrAddr2 as Address2,
    POS_ARC_D_HEAD.CrAddr3, POS_ARC_D_HEAD.CrAddr3 as Address3,
    POS_ARC_D_HEAD.ShipToAddr1, POS_ARC_D_HEAD.ShipToAddr1 as ShipAddr1,
    POS_ARC_D_HEAD.ShipToAddr2, POS_ARC_D_HEAD.ShipToAddr1 as ShipAddr2,
    POS_ARC_D_HEAD.ShipToAddr3, POS_ARC_D_HEAD.ShipToAddr1 as ShipAddr3,
    POS_ARC_D_HEAD.ShipToAddr3, POS_ARC_D_HEAD.ShipToAddr1 as ShipAddr3
FROM
    m.POS_ARC_D_HEAD as POS_ARC_D_HEAD
"""

QUERY_GET_FULL_ORDER = """
SELECT
    POS_ARC_D_SKU.SKU,
    POS_ARC_D_HEAD.PhoneNumber,
    POS_ARC_D_HEAD.CrName,
    POS_ARC_D_HEAD.Clerk,
    POS_ARC_D_HEAD.TotalAtCost,
    POS_ARC_D_HEAD.JobNumber,
    POS_ARC_D_HEAD.Customer,
    POS_ARC_D_HEAD.DocNo,
    POS_ARC_D_HEAD.DocTime,
    POS_ARC_D_HEAD.DocDate,
    POS_ARC_D_HEAD.Store,
    POS_ARC_D_HEAD.CrAddr1 AS Address1,
    POS_ARC_D_HEAD.CrAddr2 AS Address2,
    POS_ARC_D_HEAD.CrAddr3 AS Address3,
    POS_ARC_D_HEAD.ShipToName,
    POS_ARC_D_HEAD.ShipToAddr1 AS ShipAddr1,
    POS_ARC_D_HEAD.ShipToAddr2 AS ShipAddr2,
    POS_ARC_D_HEAD.ShipToAddr3 AS ShipAddr3,
    POS_ARC_D_HEAD.SpecInstructions1,
    POS_ARC_D_HEAD.SpecInstructions2,
    IN_ALT_ID.AnxLine AS LineCode,
    IN_ALT_ID.AnxKey AS ItemNumber,
    POS_ARC_D_SKU.Description,
    POS_ARC_D_SKU.QtySellingUnits AS Qty,
    POS_ARC_D_SKU.PriceExtended,
    POS_ARC_D_HEADX.ShipMeth AS Zone,
    INVENTORY1.QtyOnHand
FROM
    m.POS_ARC_D_SKU AS POS_ARC_D_SKU
INNER JOIN
    m.POS_ARC_D_HEAD AS POS_ARC_D_HEAD
    ON POS_ARC_D_SKU.DocNo = POS_ARC_D_HEAD.DocNo
    AND POS_ARC_D_SKU.Store = POS_ARC_D_HEAD.Store
INNER JOIN
    m.IN_ALT_ID AS IN_ALT_ID
    ON POS_ARC_D_SKU.SKU = IN_ALT_ID.SKU
INNER JOIN
    m.POS_ARC_D_HEADX AS POS_ARC_D_HEADX
    ON POS_ARC_D_HEAD.DocNo = POS_ARC_D_HEADX.DocNo
INNER JOIN
    m.IN_INVENTORY AS INVENTORY1
    ON INVENTORY1.SKU = POS_ARC_D_SKU.SKU
    AND INVENTORY1.Store = POS_ARC_D_HEAD.Store
WHERE
    IN_ALT_ID.AnxType = 8000
    AND POS_ARC_D_HEAD.Store NOT IN ('I', 'G', 'R')
    AND (
        (POS_ARC_D_HEAD.DocDate = ? AND POS_ARC_D_HEAD.DocTime >= ?)
        OR (POS_ARC_D_HEAD.DocDate > ?)
    )
"""

QUERY_GET_FULL_INVENTORY = """
SELECT
    IN_ALT_ID.SKU AS sku,
    IN_UPC.Upc AS upc,
    INV.Description AS description,
    IN_UPC.PackageQty AS package_quantity,
    (INV.Class || ' ' || IN_ALT_ID.AnxKey) AS item
FROM
    m.IN_INVENTORY INV
INNER JOIN
    m.IN_ALT_ID IN_ALT_ID
    ON INV.SKU = IN_ALT_ID.SKU
INNER JOIN
    m.IN_UPC IN_UPC
    ON IN_UPC.SKU = IN_ALT_ID.SKU
WHERE
    INV.STORE IN ('1','2','3') AND
    IN_ALT_ID.AnxType = 8000
"""
QUERY_TRANSFERS = """
            SELECT 
        m.POS_TRX_HEADER.Store,
        m.POS_TRX_HEADER.ShipToFlag,
        m.POS_TRX_HEADER.TrxNo,
        m.POS_TRX_HEADER.TrxType,
        m.POS_TRX_HEADER.Customer,
        m.POS_TRX_HEADER.OrderNumber,
        m.POS_TRX_HEADER.DateCreation,
        m.POS_TRX_HEADER.Clerk,
        m.POS_TRX_HEADER.TotalAtCost,
        POS_TRX_SKU.Description,
        POS_TRX_SKU.QtySellingUnits,
        POS_TRX_SKU.SKU,
        m.IN_UPC.UPC
    FROM 
        m.POS_TRX_HEADER
    INNER JOIN 
        POS_TRX_SKU ON m.POS_TRX_HEADER.TrxNo = POS_TRX_SKU.TrxNo
    INNER JOIN 
        m.IN_UPC ON POS_TRX_SKU.SKU = m.IN_UPC.SKU
    WHERE 
        m.POS_TRX_HEADER.DateCreation > ?
"""
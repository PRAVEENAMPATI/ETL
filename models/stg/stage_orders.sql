select o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.ordersellingprice-o.ordercustprice as orderprofit,
c.customerid,
c.customername,
c.segment,
c.country,
P.productid,
p.category,
p.productname,
p.subcategory
from 
    {{ ref('raw_orders') }} as o
    left outer join {{ ref('raw_customer') }} as c
        on o.customerid=c.customerid
    left outer join {{ ref('raw_product') }} as p
        on o.productid=p.productid
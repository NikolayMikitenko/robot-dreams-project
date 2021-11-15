create table fact_orders
(
	order_id int not null,
	product_id int not null,
	client_id int not null,
	order_date date not null,
	quantity int not null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=1)
distributed by (product_id, client_id)
;
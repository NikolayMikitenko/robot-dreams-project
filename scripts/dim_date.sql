CREATE TABLE dim_date 
(
	"date" date not null,	
	"year" int not null,
	"month" int not null,
	"day" int not null,
	day_of_week int not null,
	day_of_year int not null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by ("date")
;
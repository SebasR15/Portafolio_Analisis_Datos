-- Creamos los database necesarios para el ETL de este database.

create database brz_housing 
create database slv_housing 

use DB_HOUSING
use brz_housing

--- Creamos la tabla con los mismos valores y estructura que nos viene por defecto

create table brz_housing(
date varchar(100), 
price varchar(100),
bedrooms varchar(20),
bathrooms varchar(20),
sqft_living varchar(100),
sqft_lot varchar(20),
floors varchar(10),
waterfront varchar(10),
vista varchar(10),
condition varchar(10),
sqft_above varchar(100),
sqft_basement varchar(100),
yr_built varchar(50),
yr_renovated varchar(50),
street varchar(200),
city varchar(100),
statezip varchar(100),
country varchar(50), 
fecha_carga varchar(100)
)

-- Insertamos valores a la tabla brz_housing con los valores del origen

insert into brz_housing 
	select	a.date 
		   ,a.price
		   ,a.bedrooms
		   ,a.bathrooms
		   ,a.sqft_living 
		   ,a.sqft_lot 
		   ,a.floors 
		   ,a.waterfront 
		   ,a.[view]
		   ,a.condition
		   ,a.sqft_above
		   ,a.sqft_basement
		   ,a.yr_built
		   ,a.yr_renovated 
		   ,a.street
		   ,a.city 
		   ,a.statezip
		   ,a.country 
		   ,convert(varchar(20), getdate(),120) as fecha_carga
			from
			DB_HOUSING.dbo.[USA Housing Dataset] a 

use slv_housing
drop table dimCity
create table dimCity(
CityKey int primary key identity(1,1),
city varchar(100),
statezip varchar(100),
country varchar(100)
)

insert into dimCity(City, statezip, country) 
		select	distinct a.city
						,a.statezip
						,a.country
		from brz_housing.dbo.brz_housing a 

select * from dimCity

create table dimTime(
DateKey int primary key,
[Date] date not null, 
[year] int,
[month] int,
)

insert into dimTime(DateKey,[Date], [year], [month])
		select distinct convert(int,Format(Convert(date, a.[date]),'yyyyMMdd')),
						convert(date, a.[Date]),
						year(convert(date, a.[Date])),
						month(convert(date, a.[Date]))
						from brz_housing.dbo.brz_housing a 

create table dimFeatures(
Featureskey int primary key identity(1,1),
Bedrooms decimal(10,2),
Bathrooms decimal(10,2),
Floors decimal(10,1),
Waterfront int, 
Vista int, 
Condition int
)

insert into dimFeatures(Bedrooms, Bathrooms, Floors, Waterfront, Vista, Condition)
		select distinct convert(float, a.bedrooms), 
						convert(float, a.Bathrooms), 
						convert(float, a.floors), 
						convert(int, a.waterfront), 
						convert(int, a.vista), 
						convert(int, a.condition)
		from brz_housing.dbo.brz_housing a 


create table fctSales(
SalesKey int primary key identity(1,1),
Price decimal(10,2), 
Sqft_living int, 
Sqft_lot int,
Sqft_above int,
Sqft_basement int, 
yr_built int, 
yr_renovated int, 
FeaturesKey int foreign key references dimFeatures(FeaturesKey),
CityKey int foreign key references dimCity(CityKey),
dateKey int foreign key references dimTime(dateKey)
)


insert into fctSales(Price, Sqft_living, Sqft_lot, Sqft_above, Sqft_basement, yr_built, yr_renovated, FeaturesKey, CityKey, dateKey)
	select distinct convert(decimal(18,2), a.price),
					convert(int, a.sqft_living),
					convert(int, a.Sqft_lot),
					convert(int, a.sqft_above),
					convert(int, a.sqft_basement),
					convert(int, a.yr_built),
					convert(int, a.yr_renovated),
					f.featureskey, 
					c.citykey,
					t.DateKey
			from brz_housing.dbo.brz_housing a 
			inner join dimFeatures F on
						f.Bathrooms= a.bathrooms and
						f.bedrooms = a.bedrooms and
						f.floors = a.floors and 
						f.waterfront = a.waterfront and 
						f.vista = a.vista and 
						f.condition = a.condition 

			inner join dimCity c on 
						c.city = a.city and 
						c.statezip = a.statezip and 
						c.country = a.country 
			inner join dimTime t on 
						t.[Date] = convert(DATE, a.[date]);



select * from dimTime
select * from brz_housing.dbo.brz_housing 

select price, sqft_living, sqft_lot, sqft_above, sqft_basement, yr_built, yr_renovated from DB_HOUSING.dbo.[USA Housing Dataset]

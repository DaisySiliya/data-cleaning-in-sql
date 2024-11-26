-- data cleaning

 select *
from layoffs;

-- 1. Remove duplicates
-- 2. standardise the data
-- 3. null values or blank values
-- 4. remove any columns 
-- N/B its adviced to not work on the raw data, create a duplicate 

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;


select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) row_num
from layoffs_staging;

with duplicate_CTE as
(
select *,
row_number() over(
partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) row_num
from layoffs_staging
)
select *
from duplicate_CTE
where row_num > 1;

select *
from layoffs_staging
where company = 'casper';   


with duplicate_CTE as
(
select *,
row_number() over(
partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) row_num
from layoffs_staging
)
delete 
from duplicate_CTE
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num >1
;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2
where row_num >1
;

select *
from layoffs_staging2;

-- standardizing data finding and fixing issues in your data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select distinct industry
from layoffs_staging2
;

update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';

select distinct location
from layoffs_staging2
order by 1
;
select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'united states'
order by 1;

select distinct country, trim( trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim( trailing '.' from country)
where country like 'united states';

select `date`,
  str_to_date(`date` ,'%m/%d/%Y') the_date
from layoffs_staging2
;
update layoffs_staging2
set `date` = str_to_date(`date` ,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;


-- null values


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

update layoffs_staging2
set industry =null
where industry = '';

select *
from layoffs_staging2
where total_laid_off is null
or industry = ''
;

select *
from layoffs_staging2
where company = 'Airbnb'
;

select st1.industry, st2.industry
from layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company = st2.company
    and st1.location=st2.location
where (st1.industry is null or st1.industry = '')
and st2.industry is not null;


update layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company = st2.company
set st1.industry = st2.industry
where st1.industry is null 
and st2.industry is not null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;
delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;
select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
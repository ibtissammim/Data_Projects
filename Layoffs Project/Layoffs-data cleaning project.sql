-- Data Cleaning

-- In this script, we are preparing the data for use in an exploratory data analysis project. 
-- We will perform four main data cleaning steps:
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Handle Null or Blank Values
-- 4. Remove Unnecessary Columns

-- Step 1: Remove Duplicates

-- Select all records from the layoffs table
SELECT *
FROM layoffs;

-- Create a staging table similar to layoffs
CREATE TABLE layoffs_staging 
LIKE layoffs;

-- Insert records into the staging table
INSERT INTO layoffs_staging 
SELECT *
FROM layoffs;

-- Identify and number duplicate records
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
                        country, funds_raised_millions) AS row_num
FROM layoffs_staging;


-- Create a new staging table with additional row_num column
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT 
);

-- Insert records into the new staging table with row_num for duplicates
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
                        country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Delete duplicate rows from the new staging table
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 2: Standardize the Data

-- Trim whitespace from company names
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Standardize industry names containing 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Trim trailing periods from country names
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert date strings to date type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the date column to be of DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 3: Handle Null or Blank Values

-- Remove blank industry values
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

-- Fill null industry values based on matching company records
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Delete records where both total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Step 4: Remove Unnecessary Columns

-- Drop the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final selection to verify data
SELECT * 
FROM layoffs_staging2;



















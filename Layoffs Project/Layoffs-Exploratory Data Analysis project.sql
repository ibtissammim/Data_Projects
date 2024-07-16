-- Exploratory Data Analysis

-- Selecting all columns from the layoffs_staging2 table
SELECT *
FROM layoffs_staging2;

-- Selecting all columns where the percentage laid off is 100%, ordered by funds raised in descending order
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Summing total laid off employees grouped by company, ordered by total laid off in descending order
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC;

-- Finding the earliest and latest layoff dates
SELECT MIN(`date`) AS earliest_date, MAX(`date`) AS latest_date
FROM layoffs_staging2;

-- Summing total laid off employees grouped by industry, ordered by total laid off in descending order
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- Summing total laid off employees grouped by year, ordered by total laid off in descending order
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY total_laid_off DESC;

-- Summing total laid off employees grouped by stage, ordered by total laid off in descending order
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC;

-- Rolling total layoffs
-- Summing total laid off employees grouped by month, ordered by month in ascending order
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC;

-- Calculating rolling total layoffs using a common table expression (CTE)
WITH rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`
    ORDER BY `month` ASC
)
SELECT `month`, total_off,
       SUM(total_off) OVER (ORDER BY `month`) AS rolling_total
FROM rolling_total;

-- Summing total laid off employees grouped by company, ordered by total laid off in descending order
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC;

-- Summing total laid off employees grouped by company and year, ordered by total laid off in descending order
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total_laid_off DESC;

-- Ranking companies by total laid off employees within each year using common table expressions (CTEs)
WITH company_year AS (
    SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
company_year_rank AS (
    SELECT *, 
           DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE year IS NOT NULL
)
SELECT * 
FROM company_year_rank
WHERE ranking <= 5;

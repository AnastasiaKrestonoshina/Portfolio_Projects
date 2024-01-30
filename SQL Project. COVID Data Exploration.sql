-- Посмотрим на данные двух таблиц, которые мы будем использовать
-- Отсортируем их по стране и дате
-- Отберем только записи заболеваний по странам, а не по всему миру или отдельным регионам

SELECT *
FROM project1..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4;

SELECT *
FROM project1..CovidVaccination
WHERE continent IS NOT NULL
ORDER BY 3,4;

-- Отбираем данные, которые будем использовать

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM project1..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2;

-- Сравним общее количество случаев заболевания (Total Cases) с количеством смертей (Total Deaths)
-- Для этого сначала изменим тип рассматриваемых данных:

ALTER TABLE project1..CovidDeaths
ALTER COLUMN total_cases FLOAT;

ALTER TABLE project1..CovidDeaths
ALTER COLUMN total_deaths FLOAT;

ALTER TABLE project1..CovidDeaths
ALTER COLUMN population FLOAT;

-- Посчитаем, сколько смертей пришлось на количество заболевших (вероятность умереть при заболевании COVID-19) в России
-- Округлим это число до двух знаков после запятой

SELECT location, date, total_cases, total_deaths, 
	ROUND((total_deaths/total_cases) * 100, 2) AS deaths_percentage
FROM project1..CovidDeaths
WHERE location like 'Russia'
ORDER BY 1, 2;

-- Теперь посмотрим на соотношение числа заболевших к размеру населения в России
-- Из этого запроса можно посмотреть на то, какой процент населения переболел коронавирусом

SELECT location, date, population, total_cases, 
	ROUND((total_cases/population) * 100, 2) AS percent_population_infected
FROM project1..CovidDeaths
WHERE location like '%states%'
ORDER BY 1, 2;

-- Узнаем максимальный процент заболевших по всем странам
-- Также можем посмотреть только на достаточно большие страны с населением не менее 10 миллионов человек
-- Для этого группируем выборку по стране и населнию

SELECT location, population,
	MAX(total_cases) AS highest_infection_count,
	ROUND(MAX((total_cases / population) * 100), 2) AS percent_population_infected
FROM project1..CovidDeaths
WHERE  continent IS NOT NULL 
	-- AND population > 10000000
GROUP BY location, population
ORDER BY percent_population_infected DESC;

-- Посмотрим на статистику смертности от коронавируса по всем странам

SELECT location,
	MAX(CAST(total_deaths AS INT)) AS total_deaths_count
FROM project1..CovidDeaths
WHERE  continent IS NOT NULL 
GROUP BY location
ORDER BY total_deaths_count DESC;

-- Посмотрим на статистику смертности от коронавируса по регионам

SELECT location,
	MAX(CAST(total_deaths AS INT)) AS total_deaths_count
FROM project1..CovidDeaths
WHERE  continent IS NULL 
	AND location NOT LIKE '%income%'
GROUP BY location
ORDER BY total_deaths_count DESC;

-- Посмотрим на статитиску всех случаев заболеваний или смерти от коронавируса по всему миру
-- Для того, чтобы было возможно посчитать процент смертности, избавимся также от нулевых значений

SELECT date, 
	SUM(CAST(new_cases AS DECIMAL(10, 2))) AS total_new_cases,
	SUM(CAST(new_deaths AS DECIMAL(10, 2))) AS total_new_deaths,
	CASE
		WHEN SUM(CAST(new_cases AS DECIMAL(10, 2))) = 0 THEN 0 -- Если знаменатель равен нулю
		ELSE (SUM(CAST(new_deaths AS DECIMAL(10, 2))) / NULLIF(SUM(CAST(new_cases AS DECIMAL(10, 2))), 0) * 100) -- Иначе выполнить деление
	END AS death_percentage
FROM project1..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2;

-- Посмотрим на общее количество заболевших и умерших от коронавируса и на процент смертности

SELECT 
	SUM(CAST(new_cases AS DECIMAL(10, 2))) AS total_new_cases,
	SUM(CAST(new_deaths AS DECIMAL(10, 2))) AS total_new_deaths,
	CASE
		WHEN SUM(CAST(new_cases AS DECIMAL(10, 2))) = 0 THEN 0 -- Если знаменатель равен нулю
		ELSE (SUM(CAST(new_deaths AS DECIMAL(10, 2))) / NULLIF(SUM(CAST(new_cases AS DECIMAL(10, 2))), 0) * 100) -- Иначе выполнить деление
	END AS death_percentage
FROM project1..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2;

-- Соотношение населения и вакцинированных

SELECT 
	Deaths.continent, 
	Deaths.location, 
	Deaths.date, 
	Deaths.population, 
	Vaccines.new_vaccinations,
	SUM(CONVERT(DECIMAL(10, 2), Vaccines.new_vaccinations)) 
		OVER (PARTITION BY Deaths.location ORDER BY Deaths.location, Deaths.date) AS rolling_people_vaccinated
FROM 
	project1..CovidDeaths AS Deaths
JOIN 
	project1..CovidVaccination AS Vaccines
	ON Deaths.location = Vaccines.location
	AND Deaths.date = Vaccines. date
WHERE 
	Deaths.continent IS NOT NULL
ORDER BY
    Deaths.location,
    Deaths.date;

-- Используя CTE, можем посмотреть на процент вакцинаций 
-- (но: кто-то мог вакцинироваться два и более раз, 
--	 поэтому это процент всех вакцинаций по отношению к населению)

WITH Population_VS_vaccination 
	(continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
AS
(
	SELECT 
		Deaths.continent, 
		Deaths.location, 
		Deaths.date, 
		Deaths.population, 
		Vaccines.new_vaccinations,
		SUM(CONVERT(DECIMAL(10, 2), Vaccines.new_vaccinations)) 
			OVER (PARTITION BY Deaths.location ORDER BY Deaths.location, Deaths.date) AS rolling_people_vaccinated
	FROM 
		project1..CovidDeaths AS Deaths
	JOIN 
		project1..CovidVaccination AS Vaccines
		ON Deaths.location = Vaccines.location
		AND Deaths.date = Vaccines. date
	WHERE 
		Deaths.continent IS NOT NULL
	)

SELECT *, ROUND((rolling_people_vaccinated / population) * 100, 2) AS percentage_people_vaccinated
FROM Population_VS_vaccination
WHERE location LIKE 'Russia'

-- Воспользуемся временной таблицей, чтобы производить вычисления по PARTITION BY 

DROP TABLE IF EXISTS #percent_vaccinations
CREATE TABLE #percent_vaccinations
(
	continent NVARCHAR(255),
	location NVARCHAR(255),
	date DATETIME,
	population NUMERIC,
	new_vaccinations NUMERIC,
	rolling_people_vaccinated NUMERIC
)

INSERT INTO #percent_vaccinations
SELECT 
	Deaths.continent, 
	Deaths.location, 
	Deaths.date, 
	Deaths.population, 
	Vaccines.new_vaccinations,
	SUM(CONVERT(DECIMAL(10, 2), Vaccines.new_vaccinations)) 
		OVER (PARTITION BY Deaths.location ORDER BY Deaths.location, Deaths.date) AS rolling_people_vaccinated
FROM 
	project1..CovidDeaths AS Deaths
JOIN 
	project1..CovidVaccination AS Vaccines
	ON Deaths.location = Vaccines.location
	AND Deaths.date = Vaccines. date
WHERE 
	Deaths.continent IS NOT NULL

SELECT *, (rolling_people_vaccinated / population) * 100 AS percentage_people_vaccinated
FROM #percent_vaccinations
WHERE location LIKE 'Russia'

-- Создадим представление данных для последующих визуализаций

CREATE VIEW percent_vaccinations AS
SELECT 
	Deaths.continent, 
	Deaths.location, 
	Deaths.date, 
	Deaths.population, 
	Vaccines.new_vaccinations,
	SUM(CONVERT(DECIMAL(10, 2), Vaccines.new_vaccinations)) 
		OVER (PARTITION BY Deaths.location ORDER BY Deaths.location, Deaths.date) AS rolling_people_vaccinated
FROM 
	project1..CovidDeaths AS Deaths
JOIN 
	project1..CovidVaccination AS Vaccines
	ON Deaths.location = Vaccines.location
	AND Deaths.date = Vaccines. date
WHERE 
	Deaths.continent IS NOT NULL;

SELECT *
FROM percent_vaccinations;

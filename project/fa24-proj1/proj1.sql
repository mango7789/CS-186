-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER BY namefirst, namelast
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthYear, AVG(height) AS avgheight, COUNT(*) AS count
  FROM people
  GROUP BY birthYear
  ORDER BY birthYear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT *
  FROM q1iii
  WHERE avgheight > 70
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, people.playerid AS playerid, yearid
  FROM people
  JOIN HallofFame
  ON people.playerid = HallofFame.playerid
  WHERE inducted = 'Y'
  ORDER BY yearid DESC, playerid
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, q2i.playerid AS playerid, schoolid, yearid
  FROM q2i
  JOIN CollegePlaying
  ON q2i.playerid = CollegePlaying.playerid
  WHERE schoolid IN (
    SELECT schoolid
    FROM schools
    WHERE schoolState = 'CA'
  )
  ORDER BY yearid DESC, schoolid, playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT q2i.playerid AS playerid, namefirst, namelast, schoolid
  FROM q2i
  LEFT JOIN CollegePlaying
  ON q2i.playerid = CollegePlaying.playerid
  ORDER BY playerid DESC, schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT 
    people.playerid AS playerid, namefirst, namelast, yearid, 
    ROUND((H + H2B + 2*H3B + 3*HR) / CAST(AB AS FLOAT), 4) AS slg
  FROM people
  JOIN batting
  ON people.playerid = batting.playerid
  WHERE AB > 50
  ORDER BY slg DESC
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT 
    people.playerid, namefirst, namelast, 
    ROUND((H + H2B + 2*H3B + 3*HR) / CAST(AB AS FLOAT), 4) AS lslg
  FROM people
  JOIN (
    SELECT 
      playerid, SUM(H) AS H, SUM(H2B) AS H2B, 
      SUM(H3B) AS H3B, SUM(HR) AS HR, SUM(AB) AS AB
    FROM batting
    GROUP BY playerid 
  ) AS tmp
  ON people.playerid = tmp.playerid
  WHERE AB > 50
  ORDER BY lslg DESC
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT 
    namefirst, namelast, ROUND((H + H2B + 2*H3B + 3*HR) / CAST(AB AS FLOAT), 4) AS lslg
  FROM people
  JOIN (
    SELECT 
      playerid, SUM(H) AS H, SUM(H2B) AS H2B, 
      SUM(H3B) AS H3B, SUM(HR) AS HR, SUM(AB) AS AB
    FROM batting
    GROUP BY playerid 
  ) AS tmp
  ON people.playerid = tmp.playerid
  WHERE lslg > (
    SELECT 
      ROUND((SUM(H) + SUM(H2B) + 2*SUM(H3B) + 3*SUM(HR)) / CAST(SUM(AB) AS FLOAT), 4) AS lslg
    FROM batting
    WHERE playerid = 'mayswi01'
    GROUP BY playerid
  ) AND AB > 50
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH bins AS (
    SELECT 
      min + (max - min) * binid / 10 AS low,
      min + (max - min) * (binid + 1) / 10 AS high,
      binid
    FROM q4i, (
      SELECT 0 AS binid
      UNION ALL SELECT 1
      UNION ALL SELECT 2
      UNION ALL SELECT 3
      UNION ALL SELECT 4
      UNION ALL SELECT 5
      UNION ALL SELECT 6
      UNION ALL SELECT 7
      UNION ALL SELECT 8
      UNION ALL SELECT 9
    ) AS bin_ids
    WHERE yearid = 2016
  )
  SELECT 
    binid, low, high, (
      SELECT COUNT(*)
      FROM salaries
      WHERE yearid = 2016 AND salary >= bins.low AND (salary < bins.high OR (binid = 9 AND salary <= bins.high))
    ) AS count
  FROM bins
  ORDER BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH cte AS (
    SELECT 
      yearid, 
      MIN(salary) - LAG(MIN(salary)) OVER (ORDER BY yearid) AS mindiff, 
      MAX(salary) - LAG(MAX(salary)) OVER (ORDER BY yearid) AS maxdiff, 
      AVG(salary) - LAG(AVG(salary)) OVER (ORDER BY yearid) AS avgdiff
    FROM salaries
    GROUP BY yearid
    ORDER BY yearid
  )
  SELECT * 
  FROM cte
  WHERE yearid IN (
    SELECT yearid + 1
    FROM cte
  )
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT people.playerid AS playerid, namefirst, namelast, salary, yearid
  FROM people
  JOIN salaries
  ON people.playerid = salaries.playerid
  WHERE (
    yearid = 2000 AND salary IN (
      SELECT max
      FROM q4i
      WHERE yearid = 2000
    )
  ) OR (
    yearid = 2001 AND salary IN (
      SELECT max
      FROM q4i
      WHERE yearid = 2001
    )
  )
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT allstarfull.teamid AS team, (MAX(salary) - MIN(salary)) AS diffAvg
  FROM allstarfull
  JOIN salaries
  ON allstarfull.playerid = salaries.playerid
  WHERE salaries.yearid = 2016 AND allstarfull.yearid = 2016
  GROUP BY team
;


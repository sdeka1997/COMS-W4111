DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/
Select 1
;
/*SAMPLE ANSWER*/
SELECT MAX(salary) as Max_Salary
FROM Salaries;

/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
SELECT nameFirst, nameLast,  nameGiven 
from lahman2019clean.people
where height > 72
;

/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
Create Table JOHNS
SELECT nameFirst, nameLast, playerID, birthState
from lahman2019clean.people
where nameFirst = 'John' and birthCountry = 'USA' 
and playerID in
(SELECT distinct playerID 
FROM lahman2019clean.collegeplaying
where schoolID = 'fordham')
;

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
SET SQL_SAFE_UPDATES = 0;
Delete From JOHNS
where playerID in
(SELECT playerID FROM (SELECT playerID, sum(RBI) as RBI
from lahman2019clean.batting
where playerID in (select playerID from JOHNS) 
group by 1) as a 
where RBI < 2)

;

/*QUESTION 4
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(birthYear, playerCount, averageHeight)
AS
  SELECT birthYear, count(*), avg(height)
  from lahman2019clean.people
  group by 1
  order by 1 desc
;

/*QUESTION 5
Using Question 4, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(birthYear, playerCount, averageHeight, averageWeight)
AS
  SELECT * FROM(
  SELECT birthYear, count(*), avg(height), avg(weight) as weight
  from lahman2019clean.people
  group by 1
  order by 1 asc) as a
  WHERE weight > 180
;



/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT People.playerID, nameFirst, nameLast, schoolID
FROM People 
JOIN (SELECT HallOfFame.playerID as playerID, schoolID
FROM HallOfFame 
JOIN(SELECT playerID, CollegePlaying.schoolID 
FROM CollegePlaying 
JOIN Schools 
ON CollegePlaying.schoolID = Schools.schoolID WHERE Schools.state = "NY") school 
ON HallOfFame.playerID = school.playerID) ny_halloffame ON People.playerID = ny_halloffame.playerID 
ORDER BY schoolID;

SET SQL_SAFE_UPDATES = 0;
UPDATE Schools 
set name_full = "Columbia University!" 
where name_full = "Columbia University"


;

/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT teamID, yearID, avg(HBP) as hbp
from (SELECT teamID, yearID, HBP
	FROM Teams
	WHERE AB > 0) a
group by 1,2
order by 3 desc
limit 100

;

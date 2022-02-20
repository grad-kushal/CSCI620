select pid, count(mid) from actor join person on pid=id join movie on movie.id=mid where dyear is null and adult is true and year = '2021' group by pid having count(mid)<5 order by count(mid) desc;

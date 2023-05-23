select store
from "epicentrRepo".availability
where type = ?
order by quantity desc
limit 1;
select t.name from (select * from (select a.before_id "First_Evolution", a.after_id "Second_Evolution", b.after_id "Third_Evolution" 
from Evolution a left join Evolution b on a.after_id = b.before_id) a where a.Third_Evolution is null) e, 
(select t.name, cp.pid from Trainer t, CatchedPokemon cp where t.id = cp.owner_id) t where e.Second_Evolution = t.pid order by t.name;
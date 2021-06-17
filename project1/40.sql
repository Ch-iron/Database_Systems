select i.hometown, i.nickname from (select t.hometown, cp.level, cp.nickname from Trainer t, CatchedPokemon cp where t.id = cp.owner_id) i, 
(select t.hometown, max(cp.level) max_level from Trainer t, CatchedPokemon cp where t.id = cp.owner_id group by hometown) m 
where i.level = m.max_level and i.hometown = m.hometown order by i.hometown;
select p.name from Pokemon p left join CatchedPokemon cp on p.id = cp.pid where cp.owner_id is null order by p.name;
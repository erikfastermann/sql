/*
- every query must be named
- separate file, alltough also an advantage for syntax highlighting etc.
- requires code gen with running database (but with caching)
- public/private possible, controlled by type name as usual

- batch queries
*/

/*
type hints for parameters (to use table type info) would always be needed,
meaning that we will not type those in a special way, also for parameters (for now)

if known: schema, table, column
-> only used to figure out if the value is nullable
-> if not known we assume NOT NULL (checked), override with comment

type overrides not supported (for now), because interop parameters <-> columns

if name differs from as, we use that as the name,
but where we store that in the result struct?
*/

-- alternative:
-- GetNameAndFriendName -> NameAndFriendName (single)

--- NameAndFriendName (single)
-- or dml (only returns an error),
-- many (returns an iterator of the type with an error each and a normal error)
select p.id, p.name, friend.name
from person as /*blub*/p
join person as friend on friend.id = p.friend_id
where p.id = ?;

/*
type NameAndFriendName struct {
    Person struct {
        ID   person.ID
        Name string
    }
    Friend struct {
        Name string
    }
}
func GetNameAndFriendName(ctx app.Context, id int64) (NameAndFriendName, error)
*/
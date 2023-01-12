/*
TODO:
- batch queries -> postgres Copy -> execMany
- specify input type of queries?
- output / input of non generated type
*/

/*
- every query must be named
- separate file, alltough also an advantage for syntax highlighting etc.
- requires code gen with running database (but with caching)
- public/private possible (not for struct fields), controlled by type name as usual
- single column returns the value directly
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

/*
original idea:
Foo
GetFoo -> Foo
Foo (mode)
GetFoo -> Foo (mode)
Foo {...}
GetFoo -> Foo {...}
-- Table and column names are always quoted with ", double to escape.
-- Some databases have different defaults, but it is the standard.
GetFoo -> Foo (mode) {1: NULL, "foo"."bar": NOTNULL}
*/

/*
other idea:
Foo | one return_struct struct_name func_name_get_prefix
Foo? | option return_struct struct_name func_name_get_prefix
Foo+ | many return_struct_iter struct_name func_name_get_prefix
!GetFoo | exec func_name
#GetFoo | one multi_return func_name
#GetFoo? | option multi_return func_name
#GetFoo+ | many return_iter func_name single_column_only


identifiers without prefix (struct return) allow specifying the func_name:
GetFoo -> Foo?

for column nullability options see original idea
*/

--- GetNameAndFriendName -> NameAndFriendName?
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
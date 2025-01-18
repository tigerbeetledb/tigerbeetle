-- Wraps table in div. This gives us more control for styling.
function Table (tbl)
    return pandoc.Div(tbl, {class = 'table'})
end
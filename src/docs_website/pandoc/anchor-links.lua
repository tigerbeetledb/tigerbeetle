-- Adds anchor links to headings with IDs.
function Header (h)
  if h.identifier ~= '' then
    local anchor_link = pandoc.Link(
      h.content,           -- content
      '#' .. h.identifier, -- href
      '',                  -- title
      {class = 'anchor'}   -- attributes
    )
    h.content = {anchor_link}
    return h
  end
end
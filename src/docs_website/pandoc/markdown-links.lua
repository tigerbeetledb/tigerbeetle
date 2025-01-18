is_readme = PANDOC_STATE.input_files[1]:sub(-9) == "README.md"

function Link (link)
  if not is_readme then
    -- We have to adjust relative links to go up one more level.
    if link.target:sub(1, 2) == "./"  then
      link.target = "." .. link.target
    elseif link.target:sub(1, 3) == "../" then
      link.target = "../" .. link.target
    end
  end

  -- Links to client documentation
  if link.target:sub(1, 12) == "/src/clients" then
    if link.target:find("README.md") then
      -- Coming from 'reference/requests/page'
      link.target = "../../.." .. link.target:sub(5)
    else
      -- Make GitHub link
      link.target = "https://github.com/tigerbeetle/tigerbeetle/blob/main" .. link.target
    end
  end

  link.target = link.target:gsub("README.md", "")
  link.target = link.target:gsub(".md", "")

  return link
end
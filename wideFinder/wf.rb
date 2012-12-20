class Hash

  def top(howmany)
    count = 0
    keys = []
    min = 0
    self.each do |key, val|
      if val > min
        keys << key
        keys.sort! do |k1, k2|
          diff = self[k2] - self[k1]
          (diff != 0) ? diff : k1 <=> k2
        end
        keys.pop if keys.length > howmany
        min = self[keys[-1]]
      end
    end
    keys
  end
end

@u_hits = {}
@u_bytes = {}
@s404s = {}
@clients = {}
@refs = {}
@count = 0

@u_hits.default = @u_bytes.default = @s404s.default =
  @clients.default = @refs.default = 0

def record(client, u, bytes, ref)
  @u_bytes[u] += bytes
  @count += 1
  if u =~  %r{^/ongoing/When/\d\d\dx/\d\d\d\d/\d\d/\d\d/[^ .]+$}
    @u_hits[u] += 1
    @clients[client] += 1
    unless (ref == '"-"' || ref =~ %r{^\"http://www.tbray.org/ongoing/})
      @refs[ref[1 .. -2]] += 1 # lose the quotes
    end
  end
end

def report(label, hash, shrink = false)
  puts "Top #{label}:"
  keys_by_count = hash.top(10)
  fmt = (shrink) ? " %9.1fM: %s\n" : " %10d: %s\n"
  keys_by_count.each do |key|
    pkey = (key.length > 60) ? key[0 .. 59] + "..." : key
    hash[key] = hash[key] / (1024.0 * 1024.0) if shrink
    printf fmt, hash[key], pkey
  end
  puts
end

ARGF.each_line do |line|
  f = line.split(/\s+/)
  next unless f[5] == '"GET'
  client, u, status, bytes, ref = f[0], f[6], f[8], f[9], f[10]

  if status == '200'
    record(client, u, bytes.to_i, ref)
  elsif status == '304'
    record(client, u, 0, ref)
  elsif status == '404'
    @s404s[u] += 1
  end
end

print "#{@u_hits.size} resources, #{@s404s.size} 404s, #{@clients.size} clients\n\n"

report('URIs by hit', @u_hits)
report('URIs by bytes', @u_bytes, true)
report('404s', @s404s)
report('client addresses', @clients)
report('referrers', @refs)

import collections
filepath = r"C:\Users\JakubTrllo\Downloads\log.log"


with open(filepath, "r") as stream:
    lines = stream.readlines()


times_per_plugin = collections.defaultdict(list)
previous = None
for line in lines:
    strip_line = line.strip()
    cols = strip_line.split("---")
    plugin_name = cols[0]
    c_time = float(cols[2])
    spent = 0.0000000001
    if previous is not None:
        spent = c_time - previous
    previous = c_time
    times_per_plugin[plugin_name].append(spent)


total_time_by_plugin = {}
for plugin, times in times_per_plugin.items():
    total_time_by_plugin[plugin] = sum(times)

for item in reversed(sorted(total_time_by_plugin.items(), key=lambda i: i[1])):
    plugin_name, time = item
    print(plugin_name, round(time, 2))

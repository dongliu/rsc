set terminal postscript eps enhanced color font 'arial,14' 
set output 'notification_latency.eps'
set auto x
set yrange [0.2:1]
set style data histogram
set style histogram cluster gap 2
set style fill solid border -1
set boxwidth 1
set grid ytics
set xlabel "Availability"
set ylabel "Notification time"
plot 'notification_latency.dat' using 2:xtic(1) ti col, '' u 3 ti col

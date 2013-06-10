set terminal postscript eps enhanced color font 'arial,14' 
set output 'snapshot.eps'
set auto x
set yrange [0:35000]
set style data histogram
set style histogram cluster gap 3
set style fill solid border -1
set boxwidth 1
set grid ytics
set xtic rotate by -45 scale 0 font 'arial,14'
set xlabel "tested programs"
set ylabel "time (miliseconds)"
plot 'snapshot.dat' using 2:xtic(1) ti col fs solid 1, '' u 3 ti col fs pattern 1, '' u 4 ti col fs pattern 2

set terminal png font "arial,8" size 500, 350
set output 'xrd.png'
set auto x
set yrange [0:5000]
set style data histogram
set style histogram cluster gap 3
set style fill solid border -1
set boxwidth 1
set grid ytics
#set xtic rotate by -45 scale 0 font ",8"
set xlabel "scan size"
set ylabel "average transfer time (miliseconds) per image"
plot 'xrd.dat' using 2:xtic(1) ti col, '' u 3 ti col

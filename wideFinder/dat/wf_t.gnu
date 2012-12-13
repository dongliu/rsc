set terminal png font "arial,8" size 500, 350
set output 'wft5120.png'
#set auto x
#set autoscale y
#set autoscale y2
unset xtics
set multiplot layout 3,1
set key autotitle column nobox samplen 1 noenhanced
set style data boxes
set yrange [0 : 1500]
plot 'wf_t5120.dat' using 2 ti col axes x1y1


set style data histogram
set style histogram cluster gap 3
set style fill solid border -1
set boxwidth 1
set xtic rotate by -45 scale 0 font ",8"
set xlabel "tested programs"
set ylabel "time (seconds)"
set y2label "LoC"
#set multiplot
plot 'wf_t5120.dat' using 2:xtic(1) ti col axes x1y1, 'wf_t5120.dat' using 3:xtic(1) ti col axes x2y2
#unset multiplot

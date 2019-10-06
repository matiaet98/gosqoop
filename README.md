# gosqoop
Herramienta para mover datos de una base sql hacia hadoop de forma batchera

gosqoop \
    --source oracle \
    --dest local \
    --connstring 10.0.0.2:1521/fisco.matinet \
    --user siper \
    --password siper \
    --format csv \
    --query "select * from thetable where ##CONDITIONS" \
    --output "/home/thetable.csv" \
    --parallelize
    --pcol "periodo" \
    --plevel 10
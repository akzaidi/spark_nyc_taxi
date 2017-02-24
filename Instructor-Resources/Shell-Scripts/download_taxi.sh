DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cat $DIR/raw_urls.txt | xargs -n 1 -P 6 wget -c -P data/
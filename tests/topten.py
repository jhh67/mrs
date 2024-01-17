#!/usr/bin/python
import sys 

# Inserts the item into its proper place in the list of top words
# An item is a (word, count) tuple.
def Insert(l, item):
    index = 0
    for (word, count) in l:
        if item[1] > count:
            break
        index += 1
    l.insert(index, item)

# List of top-ten (word, count) tuples. This is kept in sorted
# order with the highest count first.
top = []
def main(argv): 
    for line in sys.stdin:
        (word, count) = line.split()
        count = int(count)
        # If there are fewer than 10 items in the top-ten list
        # or the count of the current word is greater than the
        # last word on the list, then insert it into the list.
        if (len(top) < 10) or (count > top[-1][1]):
            Insert(top, (word, count))
            # If there are now more than 10 items in the list then
            # delete the last item.
            if len(top) > 10:
                top.pop()
    # Print the top-ten list.
    for (word, count) in top:
        print "%s\t%d" % (word, count)

if __name__ == "__main__": 
    main(sys.argv) 

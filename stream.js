class Stream {
    constructor (arr) {
        this.arr = arr;
    }

    mapValues(valueMapper) {
        return new Stream(this.arr.map((key, value)=>{
            return {key: key, value: valueMapper(value)};
        }));
    }

    branch(...predicates) {
        let branches = Array(predicates.length).fill([]);
        for (let el of this.arr) {
            for (let i = 0; i < predicates.length; i++) {
                if (predicates[i](el)) {
                    branches[i].push(el);
                    break;
                }
            }
        }
        return branches.map((streamArr) => new Stream(streamArr));
    }

    to(streamName) {
        return {
            topic: streamName,
            messages: [...this.arr]
        }
    }
}
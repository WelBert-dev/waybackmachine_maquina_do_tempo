(async () => {
    let totalHeight = 0;
    let distance = 100;
    while (totalHeight < document.body.scrollHeight) {
        window.scrollBy(0, distance);
        totalHeight += distance;
        await new Promise(resolve => setTimeout(resolve, 500));
    }
})();
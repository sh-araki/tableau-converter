const e = React.createElement;

class MyComponent extends React.Component {
    downloadGraph() {
        const svg = document.getElementById("graph").outerHTML;
        const blob = new Blob([svg], { type: "image/svg+xml" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "graph.svg";
        a.click();
        URL.revokeObjectURL(url);
    }

    render() {
        return e(
            "div",
            null,
            e("svg", { id: "graph", width: 500, height: 500, style: { border: "1px solid black" } }, "ここにグラフを表示"),
            e("button", { onClick: () => this.downloadGraph() }, "Download Graph")
        );
    }
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(e(MyComponent));
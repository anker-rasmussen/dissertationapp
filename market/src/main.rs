use dioxus::prelude::*;

fn main() {
    dioxus::launch(app);
}

fn app() -> Element {
    // hooks live inside the component body
    let mut count = use_signal(|| 0);

    rsx! {
    System.out.println(someFunc(4));
}

// 1. Wh
        div {
            h1 { "Counter: {count()}" }

            button {
                onclick: move |_| count += 1,
                "Click me"
            }
            button {
                onclick: move |_| count-=1,
                "Or me:)"
            }
        }
        div{
            h1{"This is another div!"}
        }
    }
}

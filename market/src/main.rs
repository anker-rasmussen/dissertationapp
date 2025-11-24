use dioxus::prelude::*;

fn main() {
    dioxus::launch(app);
}

fn app() -> Element {
    // hooks live inside the component body
    let mut count = use_signal(|| 0);

    rsx! {
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
    }
}

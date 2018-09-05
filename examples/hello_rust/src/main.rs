
// The rust library is designed with the experimenter in mind,
// and contains experimental features in addition to the core FuzzyLog api.
extern crate fuzzy_log;

use std::env;

use fuzzy_log::LogHandle;

fn main() {
    let servers = if env::args().len() == 1 {
        // This isn't part of the FuzzyLog client, but we start a FuzzyLog server
        // on another thread to make this code easy to run
        println!("(Running against local server)\n\n");
        fuzzy_log::start_server_thread("0.0.0.0:13229");
        vec!["127.0.0.1:13229".parse().unwrap()]

    } else {
        env::args().skip(1).map(|s| s.parse().unwrap()).collect()
    };

    print!("First we start a FuzzyLog client... ");

    let my_color: Vec<_> = [1, 3, 7].into_iter().map(|&i| i.into()).collect();

    let mut log = LogHandle::unreplicated_with_servers(&servers)
        .my_colors_chains(my_color.iter().cloned())
        .build();
    let first_append_colors = &mut [1.into()];

    // An example of basic FuzzyLog usage.
    {
        println!("FuzzyLog client started.\n");
        println!("Let's send some data");
        {
            let data = 11;
            println!("\tsending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }
        {
            let data = 12;
            println!("\tsending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }
        {
            let data = 13;
            println!("\tsending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }

        println!("and now we sync");

        let _ = log.sync_events(|e| println!("\t read {} from the log", e.data));

        println!("the sync is done!");
        println!("When a single client writes to the log, everything is totally ordered.\n");

        // But just running on one log isn't that interesting.
        // Let's add some dependencies. We'll need more clients for this.

        println!("Starting a second client, and syncing it with the log.");
        let mut second_log = LogHandle::unreplicated_with_servers(&servers)
            .my_colors_chains(my_color.iter().cloned())
            .build();
        let second_append_colors = &mut [1.into()];

        println!("Second client started.\n");
        print!("Second client getting the first client's appends... ");
        let mut events_seen = 0;
        while events_seen < 3 {
            let _ = second_log.sync_events(|_| events_seen += 1);
        }
        println!("done!\n");

        println!("Since the second client saw all of the first client's events");
        println!("all of these new events will be ordered after them.");
        {
            let data = 21;
            println!("\tsecond client sending {} to my color", data);
            second_log.simple_append(&data, second_append_colors);
        }
        {
            let data = 22;
            println!("\tsecond client sending {} to my color", data);
            second_log.simple_append(&data, second_append_colors);
        }
        {
            let data = 23;
            println!("\tsecond client sending {} to my color", data);
            second_log.simple_append(&data, second_append_colors);
        }

        println!("and clients will see them in that order.");
        {
            let mut reader = LogHandle::<i32>::unreplicated_with_servers(&servers)
                .my_colors_chains(my_color.iter().cloned())
                .build();
            let mut events_seen = 0;
            while events_seen < 6 {
                let _ = reader.sync_events(|e| {
                    println!("\t read {} from the log", e.data);
                    events_seen += 1;
                });
            }
        }

        println!("However if the original client appends new events without syncing");
        {
            let data = 14;
            println!("\tfirst client sending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }
        {
            let data = 15;
            println!("\tfirst client sending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }
        {
            let data = 16;
            println!("\tfirst client sending {} to my color", data);
            log.simple_append(&data, first_append_colors);
        }

        println!("clients can see them in any order with respect to the second clients appends.");

        let mut reader = LogHandle::<i32>::unreplicated_with_servers(&servers)
            .my_colors_chains(my_color.iter().cloned())
            .build();
        let mut events_seen = 9;
        while events_seen < 6 {
            let _ = reader.sync_events(|e| {
                println!("\t read {} from the log", e.data);
                events_seen += 1;
            });
        }
        let _ = reader.sync_events(|e| println!("\t read {} from the log", e.data));

        println!("");
    }
}

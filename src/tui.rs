use std::{
    error::Error,
    io,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, widgets::*};

#[derive(Default)]
struct Tui {
    pub vertical_scroll: u16,
    pub horizontal_scroll: u16,
}

pub fn show_in_tui(text: &str) -> Result<(), Box<dyn Error>> {
    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // create tui and run it
    let tick_rate = Duration::from_millis(250);
    let tui = Tui::default();
    let res = run_tui(&mut terminal, tui, tick_rate, text);

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{err:?}");
    }

    Ok(())
}

fn run_tui<B: Backend>(
    terminal: &mut Terminal<B>,
    mut tui: Tui,
    tick_rate: Duration,
    text: &str,
) -> io::Result<()> {
    let mut last_tick = Instant::now();
    loop {
        terminal.draw(|f| ui(f, &mut tui, text))?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match (key.code, key.modifiers) {
                    (KeyCode::Char('q'), KeyModifiers::NONE) => return Ok(()),
                    (KeyCode::Char('j') | KeyCode::Down, KeyModifiers::NONE) => {
                        tui.vertical_scroll = tui.vertical_scroll.saturating_add(1);
                    }
                    (KeyCode::Char('k') | KeyCode::Up, KeyModifiers::NONE) => {
                        tui.vertical_scroll = tui.vertical_scroll.saturating_sub(1);
                    }
                    (KeyCode::Char('h') | KeyCode::Left, KeyModifiers::NONE) => {
                        tui.horizontal_scroll = tui.horizontal_scroll.saturating_sub(1);
                    }
                    (KeyCode::Char('l') | KeyCode::Right, KeyModifiers::NONE) => {
                        tui.horizontal_scroll = tui.horizontal_scroll.saturating_add(1);
                    }
                    (KeyCode::Char('j') | KeyCode::Down, KeyModifiers::SHIFT) => {
                        tui.vertical_scroll = tui.vertical_scroll.saturating_add(20);
                    }
                    (KeyCode::Char('k') | KeyCode::Up, KeyModifiers::SHIFT) => {
                        tui.vertical_scroll = tui.vertical_scroll.saturating_sub(20);
                    }
                    (KeyCode::Char('h') | KeyCode::Left, KeyModifiers::SHIFT) => {
                        tui.horizontal_scroll = tui.horizontal_scroll.saturating_sub(20);
                    }
                    (KeyCode::Char('l') | KeyCode::Right, KeyModifiers::SHIFT) => {
                        tui.horizontal_scroll = tui.horizontal_scroll.saturating_add(20);
                    }
                    _ => {}
                }
            }
        }
        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }
}

fn ui<B: Backend>(f: &mut Frame<B>, tui: &mut Tui, text: &str) {
    let area = f.size();

    let paragraph = Paragraph::new(text)
        .gray()
        .scroll((tui.vertical_scroll as u16, tui.horizontal_scroll as u16));
    f.render_widget(paragraph, area);
}

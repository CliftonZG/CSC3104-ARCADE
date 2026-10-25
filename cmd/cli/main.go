package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
)

// ANSI color escapes for vibrant terminal rendering
const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiRed    = "\033[1;31m"
	ansiYellow = "\033[1;33m"
	ansiGreen  = "\033[1;32m"
	ansiBlue   = "\033[1;34m"
	ansiWild   = "\033[1;35m"
	ansiDim    = "\033[2m"
)

func formatColor(c uno.Colour) string {
	switch c {
	case uno.ColourRed:
		return fmt.Sprintf("%sRED%s", ansiRed, ansiReset)
	case uno.ColourYellow:
		return fmt.Sprintf("%sYELLOW%s", ansiYellow, ansiReset)
	case uno.ColourGreen:
		return fmt.Sprintf("%sGREEN%s", ansiGreen, ansiReset)
	case uno.ColourBlue:
		return fmt.Sprintf("%sBLUE%s", ansiBlue, ansiReset)
	case uno.ColourWild:
		return fmt.Sprintf("%sWILD%s", ansiWild, ansiReset)
	default:
		return string(c)
	}
}

func formatCard(c uno.Card) string {
	var colorCode string
	switch c.Colour {
	case uno.ColourRed:
		colorCode = ansiRed
	case uno.ColourYellow:
		colorCode = ansiYellow
	case uno.ColourGreen:
		colorCode = ansiGreen
	case uno.ColourBlue:
		colorCode = ansiBlue
	default:
		colorCode = ansiWild
	}
	return fmt.Sprintf("%s[%s %s]%s", colorCode, strings.ToUpper(string(c.Colour)), c.Value, ansiReset)
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Printf("\n%s%s=======================================================%s\n", ansiBold, ansiYellow, ansiReset)
	fmt.Printf("%s         🃏 WELCOME TO CSC3104 ARCADE UNO 🃏          %s\n", ansiBold, ansiReset)
	fmt.Printf("%s%s=======================================================%s\n\n", ansiBold, ansiYellow, ansiReset)

	humanPlayer := "You"
	botPlayers := []string{"Bot-Alice", "Bot-Bob", "Bot-Charlie"}
	allPlayers := []string{humanPlayer, botPlayers[0], botPlayers[1], botPlayers[2]}

	seed := time.Now().UnixNano()
	engine, err := uno.NewGameEngine("match-interactive", allPlayers, seed)
	if err != nil {
		fmt.Printf("Failed to deal game: %v\n", err)
		return
	}

	fmt.Println("Match started with 4 players: You, Bot-Alice, Bot-Bob, Bot-Charlie")
	fmt.Println("Type card number to play, 'd' to draw, 'p' to pass, 'snap' to test snapshot, or 'q' to quit.")

	for !engine.IsFinished() {
		state := engine.State()
		cur := engine.CurrentPlayer()
		top, _ := engine.TopCard()

		directionStr := "Clockwise (->)"
		if state.Direction < 0 {
			directionStr = "Counter-Clockwise (<-)"
		}

		fmt.Printf("-------------------------------------------------------\n")
		fmt.Printf("Top Card: %s  |  Color in force: %s  |  Dir: %s\n",
			formatCard(top), formatColor(state.CurrentColour), directionStr)

		// Print opponents' card counts
		var counts []string
		for _, p := range allPlayers {
			if p != humanPlayer {
				counts = append(counts, fmt.Sprintf("%s: %d cards", p, len(state.Hands[p])))
			}
		}
		fmt.Printf("Opponents: %s\n", strings.Join(counts, " | "))
		fmt.Printf("Active Player: %s%s%s\n", ansiBold, cur, ansiReset)

		if cur == humanPlayer {
			// Human Turn
			hand := state.Hands[humanPlayer]
			fmt.Printf("\nYour Hand (%d cards):\n", len(hand))
			for i, c := range hand {
				playable := ""
				if uno.IsPlayable(state, humanPlayer, c) {
					playable = fmt.Sprintf(" %s(Playable)%s", ansiGreen, ansiReset)
				}
				fmt.Printf("  [%d] %-20s%s\n", i, formatCard(c), playable)
			}

			fmt.Print("\nYour Move [index | 'd' to draw | 'p' to pass | 'q' to quit]: ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)

			switch input {
			case "q", "quit":
				fmt.Println("Thanks for playing!")
				return
			case "snap":
				snap, _ := engine.ExportSnapshot()
				chk, _ := engine.Checksum()
				fmt.Printf("%s[SNAPSHOT]%s Canonical state exported: %d bytes | SHA256: %s\n", ansiYellow, ansiReset, len(snap), chk)
				continue
			case "d", "draw":
				_, err := engine.DrawCard(humanPlayer)
				if err != nil {
					fmt.Printf("%sError:%s %v\n", ansiRed, ansiReset, err)
				} else {
					fmt.Printf("You drew a card.\n")
				}
			case "p", "pass":
				_, err := engine.PassTurn(humanPlayer)
				if err != nil {
					fmt.Printf("%sError:%s %v\n", ansiRed, ansiReset, err)
				} else {
					fmt.Printf("You passed your turn.\n")
				}
			default:
				idx, err := strconv.Atoi(input)
				if err != nil || idx < 0 || idx >= len(hand) {
					fmt.Printf("%sInvalid choice.%s Please enter a valid card index, 'd', or 'p'.\n", ansiRed, ansiReset)
					continue
				}

				chosen := hand[idx]
				var declaredCol uno.Colour
				if chosen.IsWild() {
					fmt.Print("Declare Color (r = Red, y = Yellow, g = Green, b = Blue): ")
					cInput, _ := reader.ReadString('\n')
					cInput = strings.TrimSpace(strings.ToLower(cInput))
					switch cInput {
					case "r", "red":
						declaredCol = uno.ColourRed
					case "y", "yellow":
						declaredCol = uno.ColourYellow
					case "g", "green":
						declaredCol = uno.ColourGreen
					case "b", "blue":
						declaredCol = uno.ColourBlue
					default:
						fmt.Printf("%sInvalid color choice, defaulting to Red.%s\n", ansiYellow, ansiReset)
						declaredCol = uno.ColourRed
					}
				}

				_, err = engine.PlayCard(humanPlayer, idx, declaredCol)
				if err != nil {
					fmt.Printf("%sIllegal move:%s %v\n", ansiRed, ansiReset, err)
				} else {
					fmt.Printf("You played %s!\n", formatCard(chosen))
				}
			}
		} else {
			// Automated Bot Turn
			time.Sleep(700 * time.Millisecond) // Realistic thinking pause
			hand := state.Hands[cur]
			played := false

			for idx, c := range hand {
				if uno.IsPlayable(state, cur, c) {
					var col uno.Colour
					if c.IsWild() {
						col = uno.ColourRed // Bot picks Red
					}
					if _, err := engine.PlayCard(cur, idx, col); err == nil {
						fmt.Printf("%s played %s\n", cur, formatCard(c))
						played = true
						break
					}
				}
			}

			if !played {
				if _, err := engine.DrawCard(cur); err == nil {
					fmt.Printf("%s had no playable card and drew a card.\n", cur)
				} else {
					_, _ = engine.PassTurn(cur)
				}
			}
		}
		fmt.Println()
	}

	fmt.Printf("\n%s%s=======================================================%s\n", ansiBold, ansiYellow, ansiReset)
	if engine.Winner() == humanPlayer {
		fmt.Printf("🎉🎉 %sCONGRATULATIONS! YOU WON THE UNO MATCH!%s 🎉🎉\n", ansiGreen, ansiReset)
	} else {
		fmt.Printf("💀 GAME OVER! %s won the match!\n", engine.Winner())
	}
	fmt.Printf("%s%s=======================================================%s\n\n", ansiBold, ansiYellow, ansiReset)
}

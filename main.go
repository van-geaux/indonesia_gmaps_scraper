package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/PuerkitoBio/goquery"
)

func main() {
	// List of proxies
	proxies := []string{
		"http://proxy1:port",
		"http://proxy2:port",
		"http://proxy3:port",
	}

	// URL of the website to scrape
	url := "https://example.com/news"

	for i, proxy := range proxies {
		fmt.Printf("Using proxy: %s\n", proxy)

		// Create a new context with proxy
		opts := append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.ProxyServer(proxy),
		)
		allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
		defer cancelAlloc()

		ctx, cancel := chromedp.NewContext(allocCtx)
		defer cancel()

		// Set up a timeout
		ctx, cancel = context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		// Variable to store the page HTML
		var pageHTML string

		// Run the task
		err := chromedp.Run(ctx,
			// Navigate to the URL
			chromedp.Navigate(url),

			// Scroll the specific div to the bottom to load more content
			chromedp.Evaluate(`document.querySelector('div.some-class').scrollTop = document.querySelector('div.some-class').scrollHeight`, nil),
			// Wait for content to load (adjust sleep duration as needed)
			chromedp.Sleep(3*time.Second),

			// Get the outer HTML of the body
			chromedp.OuterHTML("html", &pageHTML),
		)
		if err != nil {
			log.Printf("Failed to get page HTML with proxy %s: %v", proxy, err)
			continue
		}

		// Print the HTML (optional, for debugging)
		// fmt.Println(pageHTML)

		// Use goquery to parse the HTML
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(pageHTML))
		if err != nil {
			log.Fatalf("Error parsing HTML: %v", err)
		}

		// Find and print the titles of the articles
		doc.Find(".article-title").Each(func(i int, s *goquery.Selection) {
			title := s.Text()
			fmt.Printf("Article %d: %s\n", i+1, title)
		})
	}
}
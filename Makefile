.PHONY: build clean dry-run

build:
	@python3 build/build.py

dry-run:
	@python3 build/build.py --dry-run
	@echo ""
	@echo "Generated LaTeX:"
	@cat .build/cv.tex

clean:
	@rm -rf .build output
	@echo "🧹 Cleaned .build/ and output/"

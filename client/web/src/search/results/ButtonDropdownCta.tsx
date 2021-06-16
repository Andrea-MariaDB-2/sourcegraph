import classNames from 'classnames'
import React, { useCallback, useState } from 'react'
import { ButtonDropdown, DropdownMenu, DropdownToggle } from 'reactstrap'

import { Link } from '@sourcegraph/shared/src/components/Link'
import { TelemetryProps } from '@sourcegraph/shared/src/telemetry/telemetryService'

import styles from './ButtonDropdownCta.module.scss'

export interface ButtonDropdownCtaProps extends TelemetryProps {
    button: JSX.Element
    icon: JSX.Element
    title: string
    copyText: string
    eventLogName: string
    onToggle?: () => void
    className?: string
}

export const ButtonDropdownCta: React.FunctionComponent<ButtonDropdownCtaProps> = ({
    button,
    icon,
    title,
    copyText,
    telemetryService,
    eventLogName,
    onToggle,
    className,
}) => {
    const [isDropdownOpen, setIsDropdownOpen] = useState(false)
    const toggleDropdownOpen = useCallback(() => {
        setIsDropdownOpen(isOpen => !isOpen)
        onToggle?.()
    }, [onToggle])
    const onClick = (): void => {
        telemetryService.log(eventLogName)
    }

    return (
        <ButtonDropdown className="menu-nav-item" direction="down" isOpen={isDropdownOpen} toggle={toggleDropdownOpen}>
            <DropdownToggle
                className={classNames(
                    'btn btn-sm btn-outline-secondary mr-2 nav-link text-decoration-none',
                    className,
                    styles.toggle
                )}
                nav={true}
                caret={false}
            >
                {button}
            </DropdownToggle>
            <DropdownMenu right={true} className={styles.container}>
                <div className="d-flex mb-3">
                    <div className="d-flex align-items-center mr-3">
                        <div className={styles.icon}>{icon}</div>
                    </div>
                    <div>
                        <div className={styles.title}>
                            <strong>{title}</strong>
                        </div>
                        <div className={classNames('text-muted', styles.copyText)}>{copyText}</div>
                    </div>
                </div>
                <Link className="btn btn-primary" to="/sign-up" onClick={onClick}>
                    Sign up for Sourcegraph
                </Link>
            </DropdownMenu>
        </ButtonDropdown>
    )
}
